"""事件模式注册模块。

职责:
- 字段以稳定数字 ID 为身份;显示名称(name)可跨版本重命名,不改变字段身份;
- 每个模式版本声明字段的类型、是否必需与默认值;
- 管理员提出草稿版本,发布前必须校验所有仍活跃消费者的固定契约
  (字段仍在且类型兼容),否则拒绝发布且状态不变;
- 发布后旧版本保持 PUBLISHED:过渡期内新旧生产者都能提交有效事件,
  直到管理员显式 retire 结束过渡期;
- publish / rollback 都原子地切换"当前可见版本",失败时不产生任何状态变化。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterable, Mapping, Optional, Protocol


class FieldType(str, Enum):
    STRING = "string"
    INT = "int"
    FLOAT = "float"
    BOOL = "bool"


def type_matches(value: Any, ftype: FieldType) -> bool:
    """运行时值是否满足字段类型。注意 Python 中 bool 是 int 的子类,必须排除。"""
    ftype = FieldType(ftype)
    if ftype is FieldType.BOOL:
        return isinstance(value, bool)
    if ftype is FieldType.INT:
        return isinstance(value, int) and not isinstance(value, bool)
    if ftype is FieldType.FLOAT:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if ftype is FieldType.STRING:
        return isinstance(value, str)
    raise ValueError(f"未知字段类型: {ftype!r}")


@dataclass(frozen=True)
class FieldDef:
    """字段定义。field_id 是稳定身份;name 仅是显示名称,可安全重命名。"""

    field_id: int
    name: str
    type: FieldType
    required: bool = False
    default: Any = None  # 可选字段缺省时的填充值,类型必须与字段类型一致


class VersionStatus(Enum):
    DRAFT = "draft"          # 草稿:可发布,不接受事件
    PUBLISHED = "published"  # 已发布:接受按此版本提交的事件
    RETIRED = "retired"      # 已退役:过渡期结束,不再接受事件


@dataclass(frozen=True)
class SchemaVersion:
    """模式版本的只读快照。"""

    version: int
    fields: Mapping[int, FieldDef]
    status: VersionStatus

    def field(self, field_id: int) -> Optional[FieldDef]:
        return self.fields.get(field_id)


@dataclass(frozen=True)
class ConsumerRequirement:
    """消费者契约(注册中心视角):固定的 字段 ID -> 类型。"""

    consumer_id: str
    required_fields: Mapping[int, FieldType]


class ConsumerSource(Protocol):
    """向注册中心提供活跃消费者契约的接口(由投影模块实现)。"""

    def active_requirements(self) -> Iterable[ConsumerRequirement]:
        ...


class RegistryError(Exception):
    """注册中心错误基类。"""


class SchemaDefinitionError(RegistryError):
    """模式定义本身非法(重复字段 ID、默认值类型不符等)。"""


class VersionNotFoundError(RegistryError):
    """引用了不存在的版本。"""


class PublishError(RegistryError):
    """发布被拒绝;注册中心状态保持不变。"""


class RollbackError(RegistryError):
    """回退被拒绝;当前可见版本保持不变。"""


def _validate_fields(fields: Iterable[FieldDef]) -> dict[int, FieldDef]:
    by_id: dict[int, FieldDef] = {}
    for fd in fields:
        if not isinstance(fd.field_id, int) or isinstance(fd.field_id, bool) or fd.field_id <= 0:
            raise SchemaDefinitionError(f"字段 ID 必须为正整数,得到: {fd.field_id!r}")
        if fd.field_id in by_id:
            raise SchemaDefinitionError(f"重复的字段 ID: {fd.field_id}")
        if not fd.name:
            raise SchemaDefinitionError(f"字段 {fd.field_id} 缺少显示名称")
        ftype = FieldType(fd.type)  # 归一化(容忍传入字符串)
        if fd.default is not None and not type_matches(fd.default, ftype):
            raise SchemaDefinitionError(
                f"字段 {fd.field_id}({fd.name})的默认值 {fd.default!r} 与类型 {ftype.value} 不符"
            )
        by_id[fd.field_id] = FieldDef(fd.field_id, fd.name, ftype, fd.required, fd.default)
    if not by_id:
        raise SchemaDefinitionError("模式至少需要一个字段")
    return by_id


@dataclass
class _VersionRecord:
    version: int
    fields: dict[int, FieldDef]
    status: VersionStatus


class SchemaRegistry:
    """版本化事件模式注册中心(线程安全)。

    消费者契约通过 ConsumerSource 注入,发布与回退前都会重新校验,
    因此"类型不兼容"或"删除仍被需要的字段"的版本永远无法生效。
    """

    def __init__(self, consumer_source: Optional[ConsumerSource] = None):
        self._consumer_source = consumer_source
        self._records: dict[int, _VersionRecord] = {}
        self._current: Optional[int] = None
        self._next_version = 1
        self._lock = threading.RLock()

    # ------------------------------------------------------------ 提案
    def propose_version(self, fields: Iterable[FieldDef]) -> int:
        """提出新版模式(草稿,不可变;要修改需重新提案),返回版本号。

        字段身份由 field_id 决定:同一 field_id 换一个 name 即"重命名显示名称",
        不影响任何按 ID 固定字段的消费者。
        """
        with self._lock:
            validated = _validate_fields(fields)
            version = self._next_version
            self._next_version += 1
            self._records[version] = _VersionRecord(version, validated, VersionStatus.DRAFT)
            return version

    # ------------------------------------------------------------ 校验
    def _consumer_violations(self, fields: Mapping[int, FieldDef]) -> list[str]:
        if self._consumer_source is None:
            return []
        problems: list[str] = []
        for req in self._consumer_source.active_requirements():
            for field_id, pinned in req.required_fields.items():
                fd = fields.get(field_id)
                if fd is None:
                    problems.append(
                        f"消费者 {req.consumer_id} 仍需要字段 {field_id},但目标版本将其删除"
                    )
                elif fd.type != FieldType(pinned):
                    problems.append(
                        f"消费者 {req.consumer_id} 固定字段 {field_id} 的类型为 "
                        f"{FieldType(pinned).value},目标版本改为 {fd.type.value}"
                    )
        return problems

    def validate_consumer_pins(self, required_fields: Mapping[int, FieldType]) -> list[str]:
        """对照当前可见版本校验消费者契约(注册/升级时的 fail-fast)。

        尚未发布任何版本时无法校验,返回空列表——契约将在之后的发布时被强制。
        """
        with self._lock:
            if self._current is None:
                return []
            current = self._records[self._current]
            problems: list[str] = []
            for field_id, pinned in required_fields.items():
                fd = current.fields.get(field_id)
                if fd is None:
                    problems.append(f"字段 {field_id} 不存在于当前版本 v{current.version}")
                elif fd.type != FieldType(pinned):
                    problems.append(
                        f"字段 {field_id} 类型为 {fd.type.value},消费者固定为 {FieldType(pinned).value}"
                    )
            return problems

    # ------------------------------------------------------------ 发布
    def publish(self, version: int) -> None:
        """原子发布:先校验全部活跃消费者契约,通过后才切换可见版本。

        旧版本保持 PUBLISHED,过渡期内新旧生产者均可提交有效事件。
        校验失败抛 PublishError,注册中心状态完全不变。
        """
        with self._lock:
            rec = self._require(version)
            if rec.status is not VersionStatus.DRAFT:
                raise PublishError(
                    f"只有草稿可以发布,v{version} 当前状态为 {rec.status.value}"
                )
            problems = self._consumer_violations(rec.fields)
            if problems:
                raise PublishError("发布会破坏活跃消费者: " + "; ".join(problems))
            rec.status = VersionStatus.PUBLISHED
            self._current = version

    # ------------------------------------------------------------ 回退
    def rollback(self, to_version: int) -> None:
        """原子回退到任一仍 PUBLISHED 的历史版本。

        目标版本必须仍满足所有活跃消费者的契约,否则抛 RollbackError 且
        当前可见版本保持不变。回退只移动可见版本指针,不改变任何版本的状态。
        """
        with self._lock:
            rec = self._require(to_version)
            if rec.status is not VersionStatus.PUBLISHED:
                raise RollbackError(
                    f"只能回退到已发布版本,v{to_version} 当前状态为 {rec.status.value}"
                )
            problems = self._consumer_violations(rec.fields)
            if problems:
                raise RollbackError("回退会破坏活跃消费者: " + "; ".join(problems))
            self._current = to_version

    # ------------------------------------------------------------ 退役
    def retire(self, version: int) -> None:
        """结束某版本的过渡期:之后按该版本提交的事件将被隔离。"""
        with self._lock:
            rec = self._require(version)
            if version == self._current:
                raise RegistryError(f"v{version} 是当前可见版本,不能退役")
            if rec.status is not VersionStatus.PUBLISHED:
                raise RegistryError(
                    f"只有已发布版本可以退役,v{version} 当前状态为 {rec.status.value}"
                )
            rec.status = VersionStatus.RETIRED

    # ------------------------------------------------------------ 查询
    def current_version(self) -> Optional[SchemaVersion]:
        with self._lock:
            if self._current is None:
                return None
            return self._snapshot(self._records[self._current])

    def get_version(self, version: int) -> SchemaVersion:
        with self._lock:
            return self._snapshot(self._require(version))

    def get_acceptable(self, version: int) -> Optional[SchemaVersion]:
        """接入层专用:仅 PUBLISHED 版本可接受事件,其余返回 None。"""
        with self._lock:
            rec = self._records.get(version)
            if rec is None or rec.status is not VersionStatus.PUBLISHED:
                return None
            return self._snapshot(rec)

    def acceptable_versions(self) -> frozenset[int]:
        with self._lock:
            return frozenset(
                v for v, rec in self._records.items() if rec.status is VersionStatus.PUBLISHED
            )

    def find_field_default(self, field_id: int) -> Any:
        """在最新的已发布版本中查找字段默认值(投影补全用);找不到返回 None。"""
        with self._lock:
            for version in sorted(self._records, reverse=True):
                rec = self._records[version]
                if rec.status is VersionStatus.PUBLISHED and field_id in rec.fields:
                    return rec.fields[field_id].default
            return None

    # ------------------------------------------------------------ 内部
    def _require(self, version: int) -> _VersionRecord:
        rec = self._records.get(version)
        if rec is None:
            raise VersionNotFoundError(f"版本 v{version} 不存在")
        return rec

    @staticmethod
    def _snapshot(rec: _VersionRecord) -> SchemaVersion:
        return SchemaVersion(rec.version, dict(rec.fields), rec.status)
