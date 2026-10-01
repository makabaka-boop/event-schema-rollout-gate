"""事件模式平台:模式注册、事件接入、消费者投影三个协作模块。

模块边界:
- registry.SchemaRegistry   版本化模式注册;发布/回退原子切换可见版本,
                            发布前校验全部活跃消费者的固定契约;
- ingestion.IngestionService 事件接入;按声明版本验证,无效事件隔离,
                            有效事件与全部投影原子落库;
- projection.ConsumerRegistry / Projector / ProjectionStore
                            消费者固定字段契约,按契约生成并存储投影。

EventPlatform 是组合根:负责把三个模块接线,并在消费者注册/升级时
对照当前可见模式做 fail-fast 校验。
"""

from __future__ import annotations

from typing import Mapping

from .ingestion import (
    EventStore,
    IngestionService,
    IngestResult,
    Quarantine,
    QuarantinedEvent,
    StoredEvent,
    validate_event,
)
from .projection import (
    ConsumerError,
    ConsumerRegistry,
    ConsumerSpec,
    Projection,
    ProjectionStore,
    Projector,
)
from .registry import (
    ConsumerRequirement,
    FieldDef,
    FieldType,
    PublishError,
    RegistryError,
    RollbackError,
    SchemaDefinitionError,
    SchemaRegistry,
    SchemaVersion,
    VersionNotFoundError,
    VersionStatus,
    type_matches,
)

__all__ = [
    "ConsumerError",
    "ConsumerRegistry",
    "ConsumerRequirement",
    "ConsumerSpec",
    "EventPlatform",
    "EventStore",
    "FieldDef",
    "FieldType",
    "IngestResult",
    "IngestionService",
    "Projection",
    "ProjectionStore",
    "Projector",
    "PublishError",
    "Quarantine",
    "QuarantinedEvent",
    "RegistryError",
    "RollbackError",
    "SchemaDefinitionError",
    "SchemaRegistry",
    "SchemaVersion",
    "StoredEvent",
    "VersionNotFoundError",
    "VersionStatus",
    "type_matches",
    "validate_event",
]


class EventPlatform:
    """组合根:三个模块的接线与统一入口。"""

    def __init__(self) -> None:
        self.consumer_registry = ConsumerRegistry()
        # 注册中心发布/回退前,通过 consumer_registry 校验活跃消费者契约
        self.schema_registry = SchemaRegistry(consumer_source=self.consumer_registry)
        self.event_store = EventStore()
        self.quarantine = Quarantine()
        self.projection_store = ProjectionStore()
        self.projector = Projector(self.schema_registry)
        self.ingestion = IngestionService(
            registry=self.schema_registry,
            consumers=self.consumer_registry,
            projector=self.projector,
            events=self.event_store,
            projections=self.projection_store,
            quarantine=self.quarantine,
        )

    def register_consumer(
        self, consumer_id: str, required_fields: Mapping[int, FieldType]
    ) -> ConsumerSpec:
        """注册消费者:先对照当前可见版本校验契约,再登记。"""
        errors = self.schema_registry.validate_consumer_pins(required_fields)
        if errors:
            raise ConsumerError("消费者契约与当前模式不兼容: " + "; ".join(errors))
        return self.consumer_registry.register(consumer_id, required_fields)

    def upgrade_consumer(
        self, consumer_id: str, required_fields: Mapping[int, FieldType]
    ) -> ConsumerSpec:
        """消费者升级:以新契约替换旧契约,同样先校验再生效。"""
        errors = self.schema_registry.validate_consumer_pins(required_fields)
        if errors:
            raise ConsumerError("消费者契约与当前模式不兼容: " + "; ".join(errors))
        return self.consumer_registry.upgrade(consumer_id, required_fields)
