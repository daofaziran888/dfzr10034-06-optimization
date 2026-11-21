"""归档交接批次测试：冷热移交确认、回执幂等、热端启用冲突与释放防护。

场景锚点：
- 冷热样本分 east / west 两个分区，各 4 行，共 8 行。
- 冷端只确认了 east 的校验号 A71，west 回执中断、始终未确认。
- 归档与热端启用申请并行，无论先完成哪项操作，分析员都能说明 8 行各从哪里读。
- 同一分区重复提交归档回执只生效一次。
- 热端释放前必须验证有一份完整副本可读，东区确认不能释放西区。
"""
from __future__ import annotations

import app.store as store_mod
from app.services.cold_archive import (
    ColdArchiveService,
    P_CONFIRMED,
    P_INTERRUPTED,
    P_PENDING,
    H_ENABLED,
    H_RELEASED,
    R_BLOCKED,
    R_DONE,
    R_WAITING,
    BATCH_CLOSED,
    BATCH_OPEN,
)
from app.services.lifecycle_policy import LifecyclePolicyService

ASSET = "ASSET-EW"


def _fresh_services():
    store_mod.store.reset()
    return ColdArchiveService(), LifecyclePolicyService()


def _open_east_west(cold: ColdArchiveService, lifecycle: LifecyclePolicyService | None = None):
    """创建策略 -> 归档阶段流转 -> 沿调用链开启 east/west 各 4 行的交接批次。"""
    entry, missing = lifecycle.create_entry({
        "策略编号": "LIFE-EW-001", "目标资产": ASSET, "创建阶段": "数据准备",
    }) if lifecycle else (None, [])
    if lifecycle:
        flow = lifecycle.transition_stage(entry["id"], "归档阶段")
        assert flow["ok"] is True
        opened = lifecycle.open_archive_handoff(entry["id"], {"east": 4, "west": 4})
        batch = opened["batch"]
        # 归档任务与批次互相关联
        assert opened["ok"] is True
    else:
        opened = cold.open_handoff_batch(ASSET, {"east": 4, "west": 4})
        batch = opened["batch"]
    return batch


class TestHandoffBatchOpenAndRead:

    def test_batch_opens_with_two_partitions_four_rows_each(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        assert set(batch["partitions"]) == {"east", "west"}
        assert batch["status"] == BATCH_OPEN
        for name in ("east", "west"):
            part = batch["partitions"][name]
            assert part["expected_rows"] == 4
            assert part["status"] == P_PENDING
            assert part["checksum"] is None
            assert part["locations"]["hot"].endswith(f"/{name}")
            assert part["locations"]["cold"] is None

    def test_eight_rows_all_read_from_hot_before_any_receipt(self):
        """回执未返回时，8 行全部从热端已有存储位置读取。"""
        cold, lifecycle = _fresh_services()
        _open_east_west(cold, lifecycle)
        for name in ("east", "west"):
            plan = cold.read_partition(name, asset_code=ASSET)
            assert plan["ok"] is True
            assert len(plan["rows"]) == 4
            assert all(row["source"] == "热端" for row in plan["rows"])
            assert all(row["via"] == "已有存储位置" for row in plan["rows"])
            assert plan["hot_readable_rows"] == 4
            assert plan["cold_readable_rows"] == 0

    def test_open_batch_twice_for_same_task_is_idempotent(self):
        cold, lifecycle = _fresh_services()
        entry, _ = lifecycle.create_entry({
            "策略编号": "LIFE-IDEM-001", "目标资产": "ASSET-IDEM", "创建阶段": "数据准备",
        })
        lifecycle.transition_stage(entry["id"], "归档阶段")
        first = lifecycle.open_archive_handoff(entry["id"], {"p": 2})
        second = lifecycle.open_archive_handoff(entry["id"], {"p": 2})
        assert first["batch"]["batch_id"] == second["batch"]["batch_id"]
        assert second["reused"] is True
        assert len(cold.list_handoff_batches("ASSET-IDEM")) == 1


class TestReceiptConfirmationAndIdempotency:

    def test_east_confirmed_with_A71_west_stays_pending(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        result = cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-EAST-1", "A71", confirmed_rows=4
        )
        assert result["accepted"] is True
        assert result["effect"] == P_CONFIRMED
        assert result["checksum"] == "A71"
        # 批次仍开放：west 未确认
        assert cold.get_handoff_batch(batch["batch_id"])["status"] == BATCH_OPEN

        east = cold.get_handoff_batch(batch["batch_id"])["partitions"]["east"]
        west = cold.get_handoff_batch(batch["batch_id"])["partitions"]["west"]
        assert east["status"] == P_CONFIRMED and east["checksum"] == "A71"
        assert east["locations"]["cold"].endswith(f"/east")
        assert west["status"] == P_PENDING and west["checksum"] is None

    def test_duplicate_receipt_for_same_partition_applies_once(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        first = cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-DUP-1", "A71", confirmed_rows=4
        )
        second = cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-DUP-1", "A71", confirmed_rows=4
        )
        third = cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-DUP-1", "A99", confirmed_rows=4
        )
        assert first["accepted"] is True
        assert second["accepted"] is False
        assert third["accepted"] is False
        part = cold.get_handoff_batch(batch["batch_id"])["partitions"]["east"]
        # 校验号保持首次的 A71，后续回执不能篡改
        assert part["checksum"] == "A71"
        assert part["receipt_no"] == "RCP-DUP-1"
        # 尝试都留痕，可核对
        assert len(part["receipt_attempts"]) == 3

    def test_receipt_no_cannot_confirm_west_after_used_on_east(self):
        """回执单号全局幂等：east 用过的号不能再用于 west。"""
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        cold.submit_archive_receipt(batch["batch_id"], "east", "RCP-SHARED", "A71", confirmed_rows=4)
        reused = cold.submit_archive_receipt(
            batch["batch_id"], "west", "RCP-SHARED", "B72", confirmed_rows=4
        )
        assert reused["accepted"] is False
        west = cold.get_handoff_batch(batch["batch_id"])["partitions"]["west"]
        assert west["status"] == P_PENDING
        assert west["checksum"] is None

    def test_interrupted_receipt_blocks_west_but_keeps_hot(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-EAST-OK", "A71", confirmed_rows=4
        )
        interrupted = cold.submit_archive_receipt(
            batch["batch_id"], "west", "RCP-WEST-FAIL", "B72",
            confirmed_rows=4, fail=True, reason="冷端写入超时",
        )
        assert interrupted["effect"] == P_INTERRUPTED
        refreshed = cold.get_handoff_batch(batch["batch_id"])
        assert refreshed["status"] == BATCH_OPEN  # west 未确认，批次不能闭环
        west = refreshed["partitions"]["west"]
        assert west["status"] == P_INTERRUPTED
        assert west["reclamation"] == R_BLOCKED
        assert west["readable_copy"] is False
        # 热端继续保留，4 行仍可从热端读
        plan = cold.read_partition("west", asset_code=ASSET)
        assert all(row["source"] == "热端" for row in plan["rows"])
        assert plan["hot_readable_rows"] == 4

    def test_interrupted_partition_can_recover_with_new_receipt(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        cold.submit_archive_receipt(
            batch["batch_id"], "west", "RCP-WEST-FAIL", "B72",
            fail=True, reason="冷端写入超时",
        )
        retry = cold.submit_archive_receipt(
            batch["batch_id"], "west", "RCP-WEST-RETRY", "B73", confirmed_rows=4
        )
        assert retry["accepted"] is True
        assert retry["effect"] == P_CONFIRMED
        west = cold.get_handoff_batch(batch["batch_id"])["partitions"]["west"]
        assert west["checksum"] == "B73"
        assert west["reclamation"] == R_WAITING

    def test_row_count_mismatch_is_treated_as_interruption(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        result = cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-SHORT", "A70", confirmed_rows=3
        )
        assert result["effect"] == P_INTERRUPTED
        part = cold.get_handoff_batch(batch["batch_id"])["partitions"]["east"]
        assert part["reclamation"] == R_BLOCKED


class TestHotEnableConflictAndReleaseGuard:

    def test_hot_enable_during_archiving_keeps_all_rows_hot_readable(self):
        """归档与启用申请冲突：west 未确认时申请启用，热端 4 行仍可读。"""
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-EAST-1", "A71", confirmed_rows=4
        )
        enabled = cold.request_hot_enable(
            "west", asset_code=ASSET, reader="分析员", reason="准备读取西区"
        )
        assert enabled["granted"] is True
        assert enabled["archive_conflict"] is True
        assert enabled["hot_readable_rows"] == 4
        # 冷副本尚未确认，明确告知不能用于随后恢复
        assert enabled["recovery_copy"]["usable"] is False

        east_enable = cold.request_hot_enable("east", asset_code=ASSET)
        assert east_enable["archive_conflict"] is False
        assert east_enable["recovery_copy"]["usable"] is True

        # 8 行逐行可读且来源清楚
        east_plan = cold.read_partition("east", asset_code=ASSET)
        west_plan = cold.read_partition("west", asset_code=ASSET)
        assert all(r["source"] == "热端" for r in east_plan["rows"] + west_plan["rows"])
        total = len(east_plan["rows"]) + len(west_plan["rows"])
        assert total == 8

    def test_release_rejected_without_complete_readable_copy(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-EAST-1", "A71", confirmed_rows=4
        )
        # west 无回执，释放必须被拒绝
        rejected = cold.release_hot_partition(batch["batch_id"], "west")
        assert rejected["ok"] is False
        assert "冷副本未确认" in rejected["error"]
        west = cold.get_handoff_batch(batch["batch_id"])["partitions"]["west"]
        assert west["hot_state"] != H_RELEASED

    def test_east_confirmation_cannot_release_west(self):
        """不能用东区的确认释放西区四行。"""
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-EAST-1", "A71", confirmed_rows=4
        )
        west_release = cold.release_hot_partition(batch["batch_id"], "west")
        assert west_release["ok"] is False
        # 东区自身可以释放，且只影响东区
        east_release = cold.release_hot_partition(batch["batch_id"], "east")
        assert east_release["ok"] is True
        refreshed = cold.get_handoff_batch(batch["batch_id"])
        assert refreshed["partitions"]["east"]["hot_state"] == H_RELEASED
        assert refreshed["partitions"]["west"]["hot_state"] != H_RELEASED

    def test_release_after_confirmation_switches_reads_to_cold(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-EAST-1", "A71", confirmed_rows=4
        )
        released = cold.release_hot_partition(batch["batch_id"], "east")
        assert released["reclamation"] == R_DONE
        plan = cold.read_partition("east", asset_code=ASSET)
        assert plan["hot_readable_rows"] == 0
        assert plan["cold_readable_rows"] == 4
        assert all(row["source"] == "冷端" for row in plan["rows"])
        assert all(row["via"] == "原恢复入口" for row in plan["rows"])
        assert all(row["checksum"] == "A71" for row in plan["rows"])

    def test_hot_enable_after_release_routes_to_recovery_entry(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-EAST-1", "A71", confirmed_rows=4
        )
        cold.release_hot_partition(batch["batch_id"], "east")
        enabled = cold.request_hot_enable("east", asset_code=ASSET)
        assert enabled["granted"] is False
        assert "冷端恢复入口" in enabled["message"]
        assert enabled["recovery_copy"]["usable"] is True
        assert all(row["source"] == "冷端" for row in enabled["read_plan"]["rows"])


class TestReconciliationAndPolicyReclamation:

    def test_reconciliation_matches_hot_rows_against_checksums(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-EAST-1", "A71", confirmed_rows=4
        )
        cold.submit_archive_receipt(
            batch["batch_id"], "west", "RCP-WEST-FAIL", "B72",
            fail=True, reason="冷端写入超时",
        )
        cold.request_hot_enable("west", asset_code=ASSET)
        report = cold.batch_reconciliation(batch["batch_id"])
        assert report["ok"] is True
        assert report["total_expected_rows"] == 8
        assert report["total_hot_readable_rows"] == 8
        assert report["total_cold_confirmed_rows"] == 4
        by_name = {p["partition"]: p for p in report["partitions"]}
        assert by_name["east"]["checksum"] == "A71"
        assert by_name["east"]["partition_status"] == P_CONFIRMED
        assert by_name["west"]["partition_status"] == P_INTERRUPTED
        assert by_name["west"]["checksum"] is None

    def test_policy_reclamation_advances_per_batch(self):
        cold, lifecycle = _fresh_services()
        batch = _open_east_west(cold, lifecycle)
        policy = store_mod.store.find_by_field(
            "lifecycle_policy", "目标资产", ASSET
        )[0]
        cold.submit_archive_receipt(
            batch["batch_id"], "east", "RCP-EAST-1", "A71", confirmed_rows=4
        )
        cold.submit_archive_receipt(
            batch["batch_id"], "west", "RCP-WEST-FAIL", "B72",
            fail=True, reason="冷端写入超时",
        )
        assert policy["清退状态"] == "清退阻塞"
        assert policy["清退明细"]["west"]["reclamation"] == R_BLOCKED
        assert policy["清退明细"]["east"]["reclamation"] == R_WAITING
        assert policy["last_handoff_batch"] == batch["batch_id"]

        # west 补回执后再释放两个分区，批次清退完成
        cold.submit_archive_receipt(
            batch["batch_id"], "west", "RCP-WEST-RETRY", "B73", confirmed_rows=4
        )
        cold.release_hot_partition(batch["batch_id"], "east")
        done = cold.release_hot_partition(batch["batch_id"], "west")
        assert done["policy"]["清退状态"] == "批次已清退"
        assert policy["清退明细"]["east"]["checksum"] == "A71"
        assert policy["清退明细"]["west"]["checksum"] == "B73"

    def test_full_eight_row_accounting_either_operation_order(self):
        """无论先启用还是先归档，最终 8 行的读取来源都能完整说明。"""
        for enable_first in (True, False):
            cold, lifecycle = _fresh_services()
            batch = _open_east_west(cold, lifecycle)
            if enable_first:
                cold.request_hot_enable("east", asset_code=ASSET)
                cold.request_hot_enable("west", asset_code=ASSET)
            cold.submit_archive_receipt(
                batch["batch_id"], "east", f"RCP-E-{enable_first}", "A71",
                confirmed_rows=4,
            )
            cold.submit_archive_receipt(
                batch["batch_id"], "west", f"RCP-W-{enable_first}", "B72",
                fail=True, reason="冷端写入超时",
            )
            if not enable_first:
                cold.request_hot_enable("east", asset_code=ASSET)
                cold.request_hot_enable("west", asset_code=ASSET)

            # east 已确认可释放并切冷端，west 中断必须保留热端
            east_release = cold.release_hot_partition(batch["batch_id"], "east")
            west_release = cold.release_hot_partition(batch["batch_id"], "west")
            assert east_release["ok"] is True
            assert west_release["ok"] is False

            east = cold.read_partition("east", asset_code=ASSET)
            west = cold.read_partition("west", asset_code=ASSET)
            assert len(east["rows"]) == 4 and all(r["source"] == "冷端" for r in east["rows"])
            assert len(west["rows"]) == 4 and all(r["source"] == "热端" for r in west["rows"])
            report = cold.batch_reconciliation(batch["batch_id"])
            assert report["total_expected_rows"] == 8
            assert report["total_cold_confirmed_rows"] == 4
            assert report["total_hot_readable_rows"] == 4
            by_name = {p["partition"]: p for p in report["partitions"]}
            assert by_name["east"]["reclamation"] == R_DONE
            assert by_name["west"]["reclamation"] == R_BLOCKED
