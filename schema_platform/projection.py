"""消费者投影模块。

消费者以"字段 ID -> 类型"固定自己的读取契约;每个有效事件入库时,
为每个活跃消费者生成一份只含其所需字段的投影。

字段身份始终是数字 ID:显示名称的重命名不改变投影键;事件按旧版本
写入而缺少消费者所需字段时,用最新已发布版本的默认值补全,保证投影
始终满足消费者契约。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Mapping

from .registry import (
    ConsumerRequirement,
    FieldType,
    SchemaRegistry,
    type_matches,
)


class ConsumerError(Exception):
    """消费者注册/升级被拒绝。"""


@dataclass(frozen=True)
class ConsumerSpec:
    """消费者契约:固定的 字段 ID -> 类型。"""

    consumer_id: str
    required_fields: Mapping[int, FieldType]


def _normalize_pins(required_fields: Mapping[int, FieldType]) -> dict[int, FieldType]:
    if not required_fields:
        raise ConsumerError("消费者至少需要固定一个字段")
    pins: dict[int, FieldType] = {}
    for field_id, ftype in required_fields.items():
        if not isinstance(field_id, int) or isinstance(field_id, bool) or field_id <= 0:
            raise ConsumerError(f"非法字段 ID: {field_id!r}")
        pins[field_id] = FieldType(ftype)  # 容忍传入字符串,归一化为枚举
    return pins


class ConsumerRegistry:
    """消费者登记处:注册、升级(重新固定契约)、停用/启用。

    实现注册中心的 ConsumerSource 协议——发布与回退前,注册中心通过
    active_requirements() 拿到全部仍活跃消费者的契约做校验。
    """

    def __init__(self) -> None:
        self._specs: dict[str, ConsumerSpec] = {}
        self._active: set[str] = set()
        self._lock = threading.RLock()

    def register(self, consumer_id: str, required_fields: Mapping[int, FieldType]) -> ConsumerSpec:
        with self._lock:
            if consumer_id in self._specs:
                raise ConsumerError(f"消费者 {consumer_id} 已存在")
            spec = ConsumerSpec(consumer_id, _normalize_pins(required_fields))
            self._specs[consumer_id] = spec
            self._active.add(consumer_id)
            return spec

    def upgrade(self, consumer_id: str, required_fields: Mapping[int, FieldType]) -> ConsumerSpec:
        """消费者升级:以新的字段契约替换旧契约(消费者 ID 不变)。"""
        with self._lock:
            if consumer_id not in self._specs:
                raise ConsumerError(f"消费者 {consumer_id} 不存在")
            spec = ConsumerSpec(consumer_id, _normalize_pins(required_fields))
            self._specs[consumer_id] = spec
            return spec

    def deactivate(self, consumer_id: str) -> None:
        """停用:不再生成新投影,也不再约束发布/回退。"""
        with self._lock:
            self._require(consumer_id)
            self._active.discard(consumer_id)

    def activate(self, consumer_id: str) -> None:
        with self._lock:
            self._require(consumer_id)
            self._active.add(consumer_id)

    def is_active(self, consumer_id: str) -> bool:
        with self._lock:
            return consumer_id in self._active

    def get(self, consumer_id: str) -> ConsumerSpec:
        with self._lock:
            return self._require(consumer_id)

    def active_consumers(self) -> list[ConsumerSpec]:
        with self._lock:
            return [self._specs[cid] for cid in sorted(self._active)]

    def active_requirements(self) -> list[ConsumerRequirement]:
        """供注册中心在发布/回退前校验(ConsumerSource 协议)。"""
        return [
            ConsumerRequirement(spec.consumer_id, dict(spec.required_fields))
            for spec in self.active_consumers()
        ]

    def _require(self, consumer_id: str) -> ConsumerSpec:
        spec = self._specs.get(consumer_id)
        if spec is None:
            raise ConsumerError(f"消费者 {consumer_id} 不存在")
        return spec


@dataclass(frozen=True)
class Projection:
    """单个消费者对单个事件的投影,键为字段 ID。"""

    event_id: int
    fields: Mapping[int, Any]


class _Missing:
    __slots__ = ()


_MISSING = _Missing()


class Projector:
    """按消费者契约从已规范化的事件生成投影。"""

    def __init__(self, registry: SchemaRegistry) -> None:
        self._registry = registry

    def build_projection(
        self, consumer: ConsumerSpec, normalized_event: Mapping[int, Any]
    ) -> dict[int, Any]:
        projection: dict[int, Any] = {}
        for field_id, pinned in consumer.required_fields.items():
            value = normalized_event.get(field_id, _MISSING)
            if value is not _MISSING and type_matches(value, pinned):
                projection[field_id] = value
            else:
                # 事件按旧版本写入、该字段当时不存在(或值与契约类型不符):
                # 用最新已发布版本的默认值补全,保证投影满足消费者契约。
                projection[field_id] = self._registry.find_field_default(field_id)
        return projection


class ProjectionStore:
    """按消费者分区的投影存储(线程安全,只追加)。"""

    def __init__(self) -> None:
        self._by_consumer: dict[str, list[Projection]] = {}
        self._lock = threading.RLock()

    def append(self, consumer_id: str, projection: Projection) -> None:
        with self._lock:
            self._by_consumer.setdefault(consumer_id, []).append(projection)

    def for_consumer(self, consumer_id: str) -> list[Projection]:
        with self._lock:
            return list(self._by_consumer.get(consumer_id, ()))

    def count(self, consumer_id: str) -> int:
        with self._lock:
            return len(self._by_consumer.get(consumer_id, ()))

    def total(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._by_consumer.values())
