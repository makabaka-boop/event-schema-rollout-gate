"""交错执行发布、旧版写入、消费者升级与失败回退,核对各方看到的版本与数据。

运行: python3 -m unittest discover -s tests -v
"""

import threading
import unittest

from schema_platform import (
    ConsumerError,
    EventPlatform,
    FieldDef,
    FieldType,
    PublishError,
    RegistryError,
    RollbackError,
    SchemaDefinitionError,
    VersionNotFoundError,
    VersionStatus,
)

S, I, F, B = FieldType.STRING, FieldType.INT, FieldType.FLOAT, FieldType.BOOL


def make_platform_with_v1():
    """v1: 字段 1 title(string,必需),字段 2 view_count(int,默认 0)。"""
    p = EventPlatform()
    v1 = p.schema_registry.propose_version(
        [
            FieldDef(1, "title", S, required=True),
            FieldDef(2, "view_count", I, default=0),
        ]
    )
    p.schema_registry.publish(v1)
    return p, v1


class TestSchemaDefinition(unittest.TestCase):
    def test_invalid_definitions_rejected(self):
        p = EventPlatform()
        reg = p.schema_registry
        with self.assertRaises(SchemaDefinitionError):  # 空模式
            reg.propose_version([])
        with self.assertRaises(SchemaDefinitionError):  # 重复字段 ID
            reg.propose_version([FieldDef(1, "a", S), FieldDef(1, "b", S)])
        with self.assertRaises(SchemaDefinitionError):  # 非法字段 ID
            reg.propose_version([FieldDef(0, "a", S)])
        with self.assertRaises(SchemaDefinitionError):  # 默认值类型不符
            reg.propose_version([FieldDef(1, "a", I, default="not-an-int")])

    def test_publish_only_draft_and_existing_versions(self):
        p, v1 = make_platform_with_v1()
        reg = p.schema_registry
        with self.assertRaises(PublishError):  # 已发布的不能再次发布
            reg.publish(v1)
        with self.assertRaises(VersionNotFoundError):
            reg.publish(999)


class TestRenameIdentity(unittest.TestCase):
    def test_rename_display_name_preserves_field_identity(self):
        p = EventPlatform()
        reg = p.schema_registry
        v1 = reg.propose_version([FieldDef(1, "user_name", S, required=True)])
        reg.publish(v1)
        p.register_consumer("c", {1: S})
        self.assertTrue(p.ingestion.submit(1, {1: "alice"}).ok)

        # 仅改显示名称:字段身份(数字 ID)不变,不破坏任何消费者
        v2 = reg.propose_version([FieldDef(1, "display_name", S, required=True)])
        reg.publish(v2)
        self.assertTrue(p.ingestion.submit(2, {1: "bob"}).ok)

        self.assertEqual(reg.get_version(1).fields[1].name, "user_name")
        self.assertEqual(reg.get_version(2).fields[1].name, "display_name")
        # 投影键始终是字段 ID,重命名不影响消费者读到的数据形状
        projections = [pr.fields for pr in p.projection_store.for_consumer("c")]
        self.assertEqual(projections, [{1: "alice"}, {1: "bob"}])


class TestPublishCompatibility(unittest.TestCase):
    def test_incompatible_type_change_cannot_publish(self):
        p, _ = make_platform_with_v1()
        reg = p.schema_registry
        p.register_consumer("c", {1: S, 2: I})

        bad = reg.propose_version(
            [FieldDef(1, "title", I, required=True), FieldDef(2, "view_count", I, default=0)]
        )
        with self.assertRaises(PublishError):
            reg.publish(bad)
        # 发布失败:可见版本与可接受版本集合都不变,草稿仍是草稿
        self.assertEqual(reg.current_version().version, 1)
        self.assertEqual(reg.acceptable_versions(), frozenset({1}))
        self.assertEqual(reg.get_version(bad).status, VersionStatus.DRAFT)
        # 旧版写入不受影响
        self.assertTrue(p.ingestion.submit(1, {1: "still fine"}).ok)

    def test_deleting_needed_field_cannot_publish(self):
        p, _ = make_platform_with_v1()
        reg = p.schema_registry
        p.register_consumer("c", {1: S, 2: I})
        bad = reg.propose_version([FieldDef(1, "title", S, required=True)])
        with self.assertRaises(PublishError):
            reg.publish(bad)
        self.assertEqual(reg.current_version().version, 1)

    def test_type_change_and_delete_allowed_when_nobody_needs_field(self):
        p, _ = make_platform_with_v1()
        reg = p.schema_registry
        p.register_consumer("c", {1: S})  # 只固定字段 1
        v2 = reg.propose_version([FieldDef(1, "title", S, required=True),
                                  FieldDef(2, "view_count", F, default=0.0)])  # int→float
        reg.publish(v2)
        v3 = reg.propose_version([FieldDef(1, "title", S, required=True)])  # 删除字段 2
        reg.publish(v3)
        self.assertEqual(reg.current_version().version, 3)

    def test_contract_first_consumer_blocks_unsatisfying_publish(self):
        p = EventPlatform()
        reg = p.schema_registry
        p.register_consumer("early", {5: S})  # 尚无已发布版本,契约先行
        v1 = reg.propose_version([FieldDef(1, "x", S, required=True)])
        with self.assertRaises(PublishError):  # 缺少消费者需要的字段 5
            reg.publish(v1)
        v2 = reg.propose_version(
            [FieldDef(1, "x", S, required=True), FieldDef(5, "y", S, default="")]
        )
        reg.publish(v2)
        self.assertEqual(reg.current_version().version, 2)


class TestConsumerRegistration(unittest.TestCase):
    def test_pins_validated_against_current_schema(self):
        p, _ = make_platform_with_v1()
        with self.assertRaises(ConsumerError):
            p.register_consumer("bad1", {9: S})  # 字段不存在
        with self.assertRaises(ConsumerError):
            p.register_consumer("bad2", {1: I})  # 固定类型与模式不符
        p.register_consumer("ok", {1: S})
        with self.assertRaises(ConsumerError):
            p.upgrade_consumer("ok", {3: S})  # 升级到尚不存在的字段
        with self.assertRaises(ConsumerError):
            p.register_consumer("ok", {1: S})  # 重复注册


class TestIngestion(unittest.TestCase):
    def test_defaults_applied_and_normalized_event_stored(self):
        p, _ = make_platform_with_v1()
        r = p.ingestion.submit(1, {1: "x"})
        self.assertTrue(r.ok)
        self.assertEqual(p.event_store.all()[0].fields, {1: "x", 2: 0})
        p.ingestion.submit(1, {1: "y", 2: 9})
        self.assertEqual(p.event_store.all()[1].fields, {1: "y", 2: 9})

    def test_invalid_event_quarantined_without_partial_projections(self):
        p, _ = make_platform_with_v1()
        p.register_consumer("a", {1: S})
        p.register_consumer("b", {1: S, 2: I})

        r = p.ingestion.submit(1, {2: "not-an-int", 42: True})  # 缺必需字段+类型错+未知字段
        self.assertFalse(r.ok)
        self.assertEqual(len(r.errors), 3)

        self.assertEqual(len(p.quarantine), 1)
        q = p.quarantine.all()[0]
        self.assertEqual(q.version, 1)
        self.assertEqual(q.payload, {2: "not-an-int", 42: True})
        # 不能留下部分投影:事件与两个消费者的投影都为空
        self.assertEqual(len(p.event_store), 0)
        self.assertEqual(p.projection_store.count("a"), 0)
        self.assertEqual(p.projection_store.count("b"), 0)

    def test_projection_failure_leaves_no_trace(self):
        p, _ = make_platform_with_v1()
        p.register_consumer("a", {1: S})

        def boom(spec, event):
            raise RuntimeError("projection exploded")

        p.projector.build_projection = boom  # 模拟投影生成中途失败
        with self.assertRaises(RuntimeError):
            p.ingestion.submit(1, {1: "x"})
        # 先算投影后落库:任何中途失败都不留事件、不留部分投影、不误隔离
        self.assertEqual(len(p.event_store), 0)
        self.assertEqual(p.projection_store.total(), 0)
        self.assertEqual(len(p.quarantine), 0)


class TestRetire(unittest.TestCase):
    def test_retire_rules(self):
        p, v1 = make_platform_with_v1()
        reg = p.schema_registry
        v2 = reg.propose_version([FieldDef(1, "title", S, required=True)])
        with self.assertRaises(RegistryError):  # 当前版本不可退役
            reg.retire(v1)
        with self.assertRaises(RegistryError):  # 草稿不可退役
            reg.retire(v2)
        reg.publish(v2)
        reg.retire(v1)
        self.assertEqual(reg.acceptable_versions(), frozenset({2}))
        with self.assertRaises(RegistryError):  # 已退役不可重复退役
            reg.retire(v1)
        # 退役版本提交的事件被隔离
        self.assertFalse(p.ingestion.submit(v1, {1: "too late"}).ok)
        self.assertEqual(len(p.event_store), 0)
        self.assertEqual(len(p.quarantine), 1)


class TestInterleavedScenario(unittest.TestCase):
    """交错执行:发布 → 旧版写入 → 消费者升级 → 失败回退 → 成功回退 → 退役,
    每一步都核对各方看到的版本与数据。"""

    def test_interleaved_publish_old_writes_upgrade_and_rollbacks(self):
        p = EventPlatform()
        reg = p.schema_registry

        # --- v1 发布,两个消费者固定各自契约
        v1 = reg.propose_version(
            [FieldDef(1, "title", S, required=True), FieldDef(2, "view_count", I, default=0)]
        )
        reg.publish(v1)
        self.assertEqual(reg.current_version().version, 1)

        # 未发布版本不可提交
        self.assertFalse(p.ingestion.submit(7, {1: "x"}).ok)
        self.assertEqual(len(p.quarantine), 1)

        p.register_consumer("analytics", {1: S, 2: I})
        p.register_consumer("search", {1: S})

        # --- v1 写入:可选字段填默认值;两个消费者各看到自己所固定的字段
        r = p.ingestion.submit(1, {1: "hello"})
        self.assertTrue(r.ok)
        self.assertEqual(p.event_store.all()[0].fields, {1: "hello", 2: 0})
        self.assertEqual(p.projection_store.for_consumer("analytics")[-1].fields,
                         {1: "hello", 2: 0})
        self.assertEqual(p.projection_store.for_consumer("search")[-1].fields, {1: "hello"})

        # --- 无效事件:隔离,事件与投影都不留痕迹
        snapshot = (len(p.event_store), p.projection_store.total(), len(p.quarantine))
        self.assertFalse(p.ingestion.submit(1, {2: "bad", 99: 1}).ok)
        self.assertEqual((len(p.event_store), p.projection_store.total(), len(p.quarantine)),
                         (snapshot[0], snapshot[1], snapshot[2] + 1))

        # --- v2:重命名字段 1(title→headline),保留字段 2,新增字段 3
        v2 = reg.propose_version(
            [
                FieldDef(1, "headline", S, required=True),
                FieldDef(2, "view_count", I, default=0),
                FieldDef(3, "tags", S, default=""),
            ]
        )
        reg.publish(v2)
        self.assertEqual(reg.current_version().version, 2)
        self.assertEqual(reg.acceptable_versions(), frozenset({1, 2}))  # 过渡期
        # 重命名不改变字段身份
        self.assertEqual(reg.get_version(1).fields[1].name, "title")
        self.assertEqual(reg.get_version(2).fields[1].name, "headline")

        # --- 类型不兼容的提案不得发布,状态不变
        bad_type = reg.propose_version(
            [FieldDef(1, "headline", I, required=True),
             FieldDef(2, "view_count", I, default=0),
             FieldDef(3, "tags", S, default="")]
        )
        with self.assertRaises(PublishError):
            reg.publish(bad_type)
        # --- 删除仍被 analytics 需要的字段 2,不得发布
        bad_delete = reg.propose_version(
            [FieldDef(1, "headline", S, required=True), FieldDef(3, "tags", S, default="")]
        )
        with self.assertRaises(PublishError):
            reg.publish(bad_delete)
        self.assertEqual(reg.current_version().version, 2)
        self.assertEqual(reg.acceptable_versions(), frozenset({1, 2}))

        # --- 过渡期:旧生产者按 v1、新生产者按 v2 提交,均有效
        self.assertTrue(p.ingestion.submit(1, {1: "old", 2: 7}).ok)
        self.assertTrue(p.ingestion.submit(2, {1: "new", 3: "a,b"}).ok)
        self.assertEqual(p.projection_store.for_consumer("analytics")[-1].fields,
                         {1: "new", 2: 0})
        self.assertEqual(p.projection_store.for_consumer("search")[-1].fields, {1: "new"})

        # --- 消费者升级:search 固定 v2 新增的字段 3
        p.upgrade_consumer("search", {1: S, 3: S})

        # --- 升级后旧版写入:字段 3 由最新已发布版本的默认值补全
        self.assertTrue(p.ingestion.submit(1, {1: "legacy"}).ok)
        self.assertEqual(p.projection_store.for_consumer("search")[-1].fields,
                         {1: "legacy", 3: ""})
        self.assertEqual(p.projection_store.for_consumer("analytics")[-1].fields,
                         {1: "legacy", 2: 0})

        # --- 失败回退:目标是草稿 / 不存在 / 不满足活跃消费者,可见版本都不变
        with self.assertRaises(RollbackError):
            reg.rollback(bad_type)
        with self.assertRaises(VersionNotFoundError):
            reg.rollback(999)
        with self.assertRaises(RollbackError):  # v1 缺少 search 需要的字段 3
            reg.rollback(1)
        self.assertEqual(reg.current_version().version, 2)

        # --- search 停用后回退成功:原子切换可见版本,v2 仍处已发布状态
        p.consumer_registry.deactivate("search")
        reg.rollback(1)
        self.assertEqual(reg.current_version().version, 1)
        self.assertEqual(reg.current_version().fields[1].name, "title")  # 回到旧显示名
        self.assertEqual(reg.acceptable_versions(), frozenset({1, 2}))

        # --- 回退后:v1 恢复为可见版本,v2 事件在退役前仍可提交
        self.assertTrue(p.ingestion.submit(1, {1: "back-on-v1"}).ok)
        self.assertTrue(p.ingestion.submit(2, {1: "still-v2"}).ok)

        # --- 退役 v2:过渡期结束,再按 v2 提交 → 隔离,无部分投影
        reg.retire(2)
        self.assertEqual(reg.acceptable_versions(), frozenset({1}))
        before = (len(p.event_store), p.projection_store.total())
        self.assertFalse(p.ingestion.submit(2, {1: "too-late"}).ok)
        self.assertEqual((len(p.event_store), p.projection_store.total()), before)
        self.assertEqual(len(p.quarantine), 3)  # 未知版本 + 无效事件 + 退役版本

        # --- 终态核对:事件按原版本记录在案
        self.assertEqual([e.version for e in p.event_store.all()], [1, 1, 2, 1, 1, 2])
        # analytics 全程活跃,看到全部 6 个事件的投影(仅其固定字段)
        self.assertEqual(
            [pr.fields for pr in p.projection_store.for_consumer("analytics")],
            [
                {1: "hello", 2: 0},
                {1: "old", 2: 7},
                {1: "new", 2: 0},
                {1: "legacy", 2: 0},
                {1: "back-on-v1", 2: 0},
                {1: "still-v2", 2: 0},
            ],
        )
        # search 停用前看到 4 个投影;升级后旧版事件的字段 3 用默认值补全
        self.assertEqual(
            [pr.fields for pr in p.projection_store.for_consumer("search")],
            [
                {1: "hello"},
                {1: "old"},
                {1: "new"},
                {1: "legacy", 3: ""},
            ],
        )


class TestConcurrency(unittest.TestCase):
    def test_concurrent_writes_and_publish_keep_projections_complete(self):
        p, v1 = make_platform_with_v1()
        reg = p.schema_registry
        p.register_consumer("c1", {1: S})
        p.register_consumer("c2", {1: S, 2: I})
        v2 = reg.propose_version(
            [
                FieldDef(1, "title", S, required=True),
                FieldDef(2, "view_count", I, default=0),
                FieldDef(3, "tags", S, default=""),
            ]
        )

        failures = []

        def writer(base):
            for i in range(50):
                r = p.ingestion.submit(1, {1: f"{base}-{i}", 2: i})
                if not r.ok:
                    failures.append(r.errors)

        threads = [threading.Thread(target=writer, args=(f"w{k}",)) for k in range(8)]
        for t in threads:
            t.start()
        reg.publish(v2)  # 与写入并发发布
        for t in threads:
            t.join()

        self.assertEqual(failures, [])
        self.assertEqual(reg.current_version().version, 2)
        n = len(p.event_store)
        self.assertEqual(n, 8 * 50)
        # 不变式:每个事件对每个活跃消费者恰好有一份投影,绝无部分投影
        for cid in ("c1", "c2"):
            projections = p.projection_store.for_consumer(cid)
            self.assertEqual(len(projections), n)
            self.assertEqual(sorted(pr.event_id for pr in projections), list(range(1, n + 1)))


if __name__ == "__main__":
    unittest.main()
