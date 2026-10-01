"""事件接入模块。

提交流程(单个锁内原子完成):
1. 解析事件声明的版本——只有 PUBLISHED 版本可接受(草稿/退役/未知版本一律隔离);
2. 按该版本的原始模式验证并规范化(可选字段填默认值);
3. 验证失败 → 事件进入隔离区,且不写入任何事件或投影;
4. 验证通过 → 先为全部活跃消费者算好投影,再一次性落库,
   任何中途失败都不会留下部分投影。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from .projection import ConsumerRegistry, Projection, ProjectionStore, Projector
from .registry import SchemaRegistry, SchemaVersion, type_matches


@dataclass(frozen=True)
class StoredEvent:
    """已入库事件:fields 已按声明版本的模式规范化(含默认值)。"""

    event_id: int
    version: int
    fields: Mapping[int, Any]


@dataclass(frozen=True)
class QuarantinedEvent:
    """被隔离的无效事件,保留原始负载与全部失败原因。"""

    version: int
    payload: Mapping[int, Any]
    reasons: tuple[str, ...]


@dataclass(frozen=True)
class IngestResult:
    ok: bool
    event_id: Optional[int] = None
    errors: tuple[str, ...] = ()


class EventStore:
    """只追加的事件存储(线程安全),event_id 单调递增。"""

    def __init__(self) -> None:
        self._events: list[StoredEvent] = []
        self._lock = threading.RLock()

    def append(self, version: int, fields: Mapping[int, Any]) -> StoredEvent:
        with self._lock:
            event = StoredEvent(len(self._events) + 1, version, dict(fields))
            self._events.append(event)
            return event

    def all(self) -> list[StoredEvent]:
        with self._lock:
            return list(self._events)

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)


class Quarantine:
    """无效事件隔离区。"""

    def __init__(self) -> None:
        self._items: list[QuarantinedEvent] = []
        self._lock = threading.RLock()

    def add(self, version: int, payload: Mapping[int, Any], reasons: list[str]) -> None:
        with self._lock:
            self._items.append(QuarantinedEvent(version, dict(payload), tuple(reasons)))

    def all(self) -> list[QuarantinedEvent]:
        with self._lock:
            return list(self._items)

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)


def validate_event(
    schema: SchemaVersion, payload: Mapping[int, Any]
) -> tuple[Optional[dict[int, Any]], list[str]]:
    """按事件声明版本的原始模式验证;通过时返回规范化事件(可选字段填默认值)。"""
    errors: list[str] = []
    normalized: dict[int, Any] = {}

    for field_id in payload:
        if field_id not in schema.fields:
            errors.append(f"未知字段 ID: {field_id!r}")

    for field_id, fd in schema.fields.items():
        if field_id in payload:
            value = payload[field_id]
            if type_matches(value, fd.type):
                normalized[field_id] = value
            else:
                errors.append(
                    f"字段 {field_id}({fd.name})应为 {fd.type.value},实际为 {value!r}"
                )
        elif fd.required:
            errors.append(f"缺少必需字段 {field_id}({fd.name})")
        else:
            normalized[field_id] = fd.default

    if errors:
        return None, errors
    return normalized, []


class IngestionService:
    """事件接入:验证 → 隔离,或(事件 + 全部投影)原子落库。"""

    def __init__(
        self,
        registry: SchemaRegistry,
        consumers: ConsumerRegistry,
        projector: Projector,
        events: EventStore,
        projections: ProjectionStore,
        quarantine: Quarantine,
    ) -> None:
        self._registry = registry
        self._consumers = consumers
        self._projector = projector
        self._events = events
        self._projections = projections
        self._quarantine = quarantine
        self._lock = threading.RLock()

    def submit(self, version: int, payload: Mapping[int, Any]) -> IngestResult:
        """生产者按所声明版本提交事件。"""
        with self._lock:
            schema = self._registry.get_acceptable(version)
            if schema is None:
                reasons = [f"版本 v{version} 不可提交(不存在、仍是草稿或已退役)"]
                self._quarantine.add(version, payload, reasons)
                return IngestResult(ok=False, errors=tuple(reasons))

            normalized, errors = validate_event(schema, payload)
            if errors or normalized is None:
                self._quarantine.add(version, payload, errors)
                return IngestResult(ok=False, errors=tuple(errors))

            # 先算完全部投影再落库:投影若失败,事件与投影都不会留下
            pending = [
                (spec.consumer_id, self._projector.build_projection(spec, normalized))
                for spec in self._consumers.active_consumers()
            ]
            event = self._events.append(version, normalized)
            for consumer_id, fields in pending:
                self._projections.append(consumer_id, Projection(event.event_id, fields))
            return IngestResult(ok=True, event_id=event.event_id)
