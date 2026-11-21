"""生命周期领域服务：数据资产从创建到销毁的全生命周期阶段定义和自动流转。

Pipeline 4: Lifecycle policy → cold archive → retention cleanup
"""
from __future__ import annotations

import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class LifecyclePolicyService(BaseModuleService):
    MODULE = "lifecycle_policy"
    ENTITY = "生命周期策略"
    OPERATION_TERM = "归档"
    LIST_FIELDS = ["策略编号", "目标资产", "创建阶段", "活跃阶段", "保留阶段", "归档阶段", "销毁条件", "最后流转"]
    REQUIRED_FIELDS = ["策略编号", "目标资产", "创建阶段"]
    STATUS_ORDER = ["草稿", "已生效", "流转中", "已停用"]
    ACTION_RULES = {"启效策略": "已生效", "手动流转": "流转中", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    STAGES = ["创建阶段", "活跃阶段", "保留阶段", "归档阶段", "销毁阶段"]

    def transition_stage(self, policy_id: int, to_stage: str) -> dict[str, Any]:
        """流转生命周期策略到指定阶段，自动触发冷归档或保留清理（Pipeline 4 入口）。"""
        policy = self.get_entry(policy_id)
        if not policy:
            return {"ok": False, "error": "策略不存在"}

        if to_stage not in self.STAGES:
            return {"ok": False, "error": f"未知阶段：{to_stage}"}

        old_stage = policy.get("最后流转", "")
        policy["最后流转"] = to_stage
        policy["status"] = "流转中"
        policy["pending"] = True

        result = {"ok": True, "policy_id": policy_id, "from": old_stage, "to": to_stage, "actions": []}

        # 归档阶段：触发冷归档
        if to_stage == "归档阶段":
            archive_task = self._trigger_cold_archive(policy)
            result["cold_archive"] = archive_task
            result["actions"].append("cold_archive")
            self.emit_event("lifecycle_archiving", {
                "policy_id": policy_id,
                "asset_code": policy.get("目标资产", ""),
                "archive_task_id": archive_task["id"] if archive_task else None,
            })

        # 销毁阶段：触发保留清理
        if to_stage == "销毁阶段":
            cleanup_task = self._trigger_retention_cleanup(policy)
            result["retention_cleanup"] = cleanup_task
            result["actions"].append("retention_cleanup")
            self.emit_event("lifecycle_cleanup", {
                "policy_id": policy_id,
                "asset_code": policy.get("目标资产", ""),
                "cleanup_task_id": cleanup_task["id"] if cleanup_task else None,
            })

        # 记录变更
        store.log_change(
            self.MODULE, policy_id, "阶段流转",
            before=old_stage, after=to_stage,
            source="pipeline_4", scope="lifecycle_policy->cold_archive/retention_job"
        )

        return result

    def _trigger_cold_archive(self, policy: dict[str, Any]) -> dict[str, Any]:
        """触发冷归档任务。"""
        task_id = store.next_id("cold_archive")
        task = {
            "id": task_id,
            "任务编号": f"COLD-{task_id:04d}",
            "源资产": policy.get("目标资产", ""),
            "归档条件": f"阶段流转到归档阶段 (policy #{policy['id']})",
            "目标存储": "对象存储-COS-archive",
            "压缩算法": "zstd-3",
            "校验方式": "SHA-256",
            "数据行数": "0",
            "执行时间": _time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "待执行",
            "pending": True,
            "abnormal": False,
            "triggered_by": "lifecycle_policy",
            "triggered_by_id": policy["id"],
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows("cold_archive").append(task)
        return task

    def open_archive_handoff(self, policy_id: int, partitions: dict[str, int]) -> dict[str, Any]:
        """沿 lifecycle_policy -> cold_archive 调用链，为策略目标资产开启归档交接批次。

        清退状态后续由 cold_archive 按交接批次推进回写本策略。
        """
        # 局部导入，避免模块加载期循环依赖
        from app.services.cold_archive import ColdArchiveService

        policy = self.get_entry(policy_id)
        if not policy:
            return {"ok": False, "error": "策略不存在"}
        asset_code = policy.get("目标资产", "")
        tasks = [
            t for t in store.rows("cold_archive")
            if t.get("源资产") == asset_code and t.get("triggered_by_id") == policy_id
        ]
        task_id = tasks[-1]["id"] if tasks else None
        result = ColdArchiveService().open_handoff_batch(
            asset_code, partitions,
            archive_task_id=task_id, policy_id=policy_id,
        )
        if result.get("ok"):
            batch_id = result["batch"]["batch_id"]
            policy["last_handoff_batch"] = batch_id
            batches = policy.setdefault("handoff_batches", [])
            if batch_id not in batches:
                batches.append(batch_id)
            policy["清退状态"] = "分区清退中"
        return result

    def _trigger_retention_cleanup(self, policy: dict[str, Any]) -> dict[str, Any]:
        """触发保留清理任务。"""
        task_id = store.next_id("retention_job")
        task = {
            "id": task_id,
            "计划编号": f"RETE-{task_id:04d}",
            "目标资产": policy.get("目标资产", ""),
            "保留天数": "730",
            "清理条件": f"阶段流转到销毁阶段 (policy #{policy['id']})",
            "预演模式": "是",
            "清理行数": "0",
            "备份路径": f"backup/{policy.get('目标资产', 'unknown')}/",
            "下次执行": _time.strftime("%Y-%m-%d", _time.localtime(_time.time() + 86400 * 30)),
            "status": "草稿",
            "pending": True,
            "abnormal": False,
            "triggered_by": "lifecycle_policy",
            "triggered_by_id": policy["id"],
            "created_at": _time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.rows("retention_job").append(task)
        return task

    def get_lifecycle_stage(self, asset_code: str) -> str | None:
        """查询资产的当前生命周期阶段。"""
        policies = store.find_by_field(self.MODULE, "目标资产", asset_code)
        if not policies:
            return None
        active = [p for p in policies if p.get("status") in ("已生效", "流转中")]
        if active:
            return active[0].get("最后流转", active[0].get("创建阶段", ""))
        return policies[0].get("最后流转", policies[0].get("创建阶段", ""))