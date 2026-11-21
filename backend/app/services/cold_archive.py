"""冷热归档领域服务：冷热数据的自动分层归档、压缩转储和完整性校验。

接收生命周期策略的归档阶段流转事件，执行数据归档操作。

可靠性增强（归档交接批次）：
- 资产分区按实际接收情况归档到可核对的「归档交接批次」（handoff batch），
  每个分区独立维护待确认 / 已确认 / 回执中断状态与冷端校验号。
- 归档回执按回执单号幂等：同一分区重复提交同号回执只生效一次。
- 热端启用申请与归档并行不互斥：热端数据在释放前始终保留，分析员可继续读取；
  释放热端前强制验证已有一份完整且可读的冷副本，未确认分区不得清退。
- 读取侧兼容已有存储位置与原恢复入口，按分区逐行说明每行从热端还是冷端读取。
- 清退状态沿着 lifecycle_policy -> cold_archive 调用链，按交接批次推进回写
  生命周期策略，东区的确认不能用于释放西区。
"""
from __future__ import annotations

import time as _time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

# 管线状态键：交接批次、回执幂等台账、热端启用登记
STATE_BATCHES = "handoff:batches"
STATE_LEDGER = "handoff:idempotency"
STATE_HOT = "handoff:hot"

# 分区状态
P_PENDING = "待确认"        # 冷端尚未返回回执
P_CONFIRMED = "已确认"      # 冷端回执已确认，校验号可读
P_INTERRUPTED = "回执中断"  # 回执失败 / 校验不通过 / 明确中断

# 热端状态
H_RETAINED = "热端保留"
H_ENABLED = "热端启用"
H_RELEASED = "热端已释放"

# 清退状态
R_PENDING = "未清退"
R_WAITING = "待清退"   # 冷副本已确认，等待释放热端
R_DONE = "已清退"
R_BLOCKED = "阻塞"     # 回执中断，清退阻塞

BATCH_OPEN = "开放"
BATCH_CLOSED = "已闭环"


def _now() -> str:
    return _time.strftime("%Y-%m-%d %H:%M:%S")


class ColdArchiveService(BaseModuleService):
    MODULE = "cold_archive"
    ENTITY = "归档任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务编号", "源资产", "归档条件", "目标存储", "压缩算法", "校验方式", "数据行数", "执行时间"]
    REQUIRED_FIELDS = ["任务编号", "源资产", "归档条件"]
    STATUS_ORDER = ["待执行", "归档中", "已完成", "校验失败"]
    ACTION_RULES = {"启动归档": "归档中", "取消归档": "已完成", "标记失败": "校验失败"}
    NEGATIVE_ACTIONS = ["标记失败"]

    # ── 原有归档能力（保持兼容） ────────────────────────────────

    def execute_archive(self, task_id: int, data_rows: int = 0, source_snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
        """执行归档操作，将数据进行压缩转储（Pipeline 4 核心方法）。"""
        task = self.get_entry(task_id)
        if not task:
            return {"ok": False, "error": "归档任务不存在"}

        # 更新任务状态为执行中
        task["status"] = "归档中"
        task["执行时间"] = _now()
        task["数据行数"] = str(data_rows)
        task["pending"] = True

        # 模拟归档计算校验和
        checksum = self._compute_checksum(task, data_rows)

        # 如果提供了源快照，则存储快照引用（原恢复入口）
        if source_snapshot:
            task["snapshot_ref"] = source_snapshot.get("snapshot_id", "")

        # 标记完成
        task["status"] = "已完成"
        task["pending"] = False
        task["checksum"] = checksum

        # 创建版本快照记录归档版本
        store.create_snapshot(f"archive-{task_id}", [self.MODULE])

        store.log_change(
            self.MODULE, task_id, "归档完成",
            before="待执行", after="已完成",
            source="pipeline_4", scope=f"cold_archive:资产{task.get('源资产', '')}"
        )

        self.emit_event("archive_completed", {
            "task_id": task_id,
            "asset_code": task.get("源资产", ""),
            "data_rows": data_rows,
            "checksum": checksum,
        })

        return {
            "ok": True,
            "task_id": task_id,
            "asset": task.get("源资产"),
            "archived_rows": data_rows,
            "checksum": checksum,
            "storage": task.get("目标存储"),
            "pipeline": "lifecycle_policy->cold_archive",
        }

    def _compute_checksum(self, task: dict[str, Any], data_rows: int) -> str:
        """计算归档数据校验和。"""
        import hashlib
        raw = f"{task.get('源资产','')}:{data_rows}:{task.get('执行时间','')}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def get_pending_archives(self) -> list[dict[str, Any]]:
        """获取待执行的归档任务。"""
        return [
            row for row in store.rows(self.MODULE)
            if row.get("status") == "待执行"
        ]

    def count_archived(self) -> int:
        """统计已归档的任务数。"""
        return sum(1 for row in store.rows(self.MODULE) if row.get("status") == "已完成")

    # ════════════════════════════════════════════════════════════
    # 归档交接批次：分区级、可核对、幂等的冷热移交
    # ════════════════════════════════════════════════════════════

    # ── 内部状态存取 ────────────────────────────────────────────

    def _load_state(self, key: str) -> dict[str, Any]:
        # 缺失时立即挂回仓库，保证同一 key 多次读取拿到同一对象
        state = store.get_pipeline_state(key)
        if state is None:
            state = {}
            store.set_pipeline_state(key, state)
        return state

    def _save_state(self, key: str, value: dict[str, Any]) -> None:
        store.set_pipeline_state(key, value)

    def _all_batches(self) -> list[dict[str, Any]]:
        return self._load_state(STATE_BATCHES).setdefault("batches", [])

    def _ledger(self) -> dict[str, dict[str, Any]]:
        return self._load_state(STATE_LEDGER).setdefault("ledger", {})

    def _hot_registry(self) -> dict[str, Any]:
        return self._load_state(STATE_HOT).setdefault("partitions", {})

    def find_handoff_batch(self, batch_id: str) -> dict[str, Any] | None:
        for batch in self._all_batches():
            if batch["batch_id"] == batch_id:
                return batch
        return None

    def _find_partition_batch(self, partition: str, asset_code: str | None = None) -> dict[str, Any] | None:
        """找到包含指定分区的最新交接批次（可按资产收窄）。"""
        candidates = [
            b for b in self._all_batches()
            if partition in b["partitions"]
            and (asset_code is None or b["asset_code"] == asset_code)
        ]
        return candidates[-1] if candidates else None

    # ── 批次开启 ────────────────────────────────────────────────

    def open_handoff_batch(
        self,
        asset_code: str,
        partitions: dict[str, int] | list[tuple[str, int]],
        *,
        archive_task_id: int | None = None,
        policy_id: int | None = None,
    ) -> dict[str, Any]:
        """按资产分区开启一个可核对的归档交接批次。

        partitions 为 ``{分区名: 预期行数}``，例如 ``{"east": 4, "west": 4}``。
        同一归档任务重复开启时幂等返回已存在的开放批次。
        """
        if isinstance(partitions, dict):
            partition_items = list(partitions.items())
        else:
            partition_items = list(partitions)
        if not partition_items:
            return {"ok": False, "error": "交接批次至少需要一个分区"}

        task = self.get_entry(archive_task_id) if archive_task_id else None
        cold_base = (task or {}).get("目标存储", "cold-hdfs")
        hot_base = self._hot_base_location(asset_code, task)
        recovery_entry = self._recovery_entry(asset_code, task)

        # 幂等：同一归档任务已存在开放批次则直接返回
        if archive_task_id is not None:
            for existing in self._all_batches():
                if existing.get("archive_task_id") == archive_task_id and existing["status"] == BATCH_OPEN:
                    return {"ok": True, "batch": existing, "reused": True}

        state = self._load_state(STATE_BATCHES)
        batches = state.setdefault("batches", [])
        seq = len(batches) + 1
        batch_id = f"HB-{seq:04d}"

        partition_map: dict[str, dict[str, Any]] = {}
        for name, expected_rows in partition_items:
            partition_map[name] = {
                "partition": name,
                "expected_rows": int(expected_rows),
                "confirmed_rows": 0,
                "status": P_PENDING,
                "receipt_no": None,
                "checksum": None,
                "confirmed_at": None,
                "receipt_attempts": [],
                "hot_state": H_RETAINED,
                "reclamation": R_PENDING,
                "readable_copy": False,
                "locations": {
                    "hot": f"{hot_base}/{name}",
                    "cold": None,
                },
                "recovery_entry": recovery_entry,
            }

        batch = {
            "batch_id": batch_id,
            "asset_code": asset_code,
            "archive_task_id": archive_task_id,
            "policy_id": policy_id,
            "status": BATCH_OPEN,
            "opened_at": _now(),
            "closed_at": None,
            "cold_storage_base": cold_base,
            "partitions": partition_map,
        }
        batches.append(batch)
        self._save_state(STATE_BATCHES, state)

        if task is not None:
            task["handoff_batch_id"] = batch_id
            task["pending"] = True

        # 热端登记：开启批次时热端数据均在
        hot_state = self._load_state(STATE_HOT)
        hot_reg = hot_state.setdefault("partitions", {})
        for name, expected_rows in partition_items:
            hot_reg[f"{asset_code}/{name}"] = {
                "asset_code": asset_code,
                "partition": name,
                "batch_id": batch_id,
                "enabled": True,
                "hot_rows": int(expected_rows),
                "enabled_by": None,
                "enabled_at": None,
                "requests": [],
            }
        self._save_state(STATE_HOT, hot_state)

        store.log_change(
            self.MODULE, archive_task_id or 0, "开启归档交接批次",
            before="-", after=batch_id,
            source="pipeline_4",
            scope=f"cold_archive:资产{asset_code}:分区{','.join(n for n, _ in partition_items)}",
        )
        self.emit_event("handoff_batch_opened", {
            "batch_id": batch_id, "asset_code": asset_code,
            "partitions": {n: r for n, r in partition_items},
        })
        return {"ok": True, "batch": batch, "reused": False}

    def _hot_base_location(self, asset_code: str, task: dict[str, Any] | None) -> str:
        """热端位置优先复用资产注册里的已有存储位置。"""
        for asset in store.rows("asset_registry"):
            if asset.get("资产编号") == asset_code and asset.get("存储位置"):
                return str(asset["存储位置"]).rstrip("/")
        return f"hdfs://hot/{asset_code}"

    def _recovery_entry(self, asset_code: str, task: dict[str, Any] | None) -> str:
        """原恢复入口：归档任务快照引用优先，其次保留清理备份路径。"""
        if task and task.get("snapshot_ref"):
            return f"snapshot://{task['snapshot_ref']}"
        backups = store.find_by_field("retention_job", "目标资产", asset_code)
        for job in backups:
            if job.get("备份路径"):
                return f"restore://{job['备份路径']}"
        return f"restore://hot/{asset_code}"

    # ── 归档回执（冷端确认） ────────────────────────────────────

    def submit_archive_receipt(
        self,
        batch_id: str,
        partition: str,
        receipt_no: str,
        checksum: str,
        *,
        confirmed_rows: int | None = None,
        readable: bool = True,
        fail: bool = False,
        reason: str = "",
    ) -> dict[str, Any]:
        """提交冷端归档回执，按回执单号幂等，只生效一次。

        - 同号回执重复提交：记录尝试但不改变分区状态，返回 ``accepted=False``。
        - 成功回执：分区置为已确认，登记校验号与冷端存储位置，进入待清退。
        - 失败 / 不可读 / 行数不符：分区置为回执中断，清退阻塞，热端继续保留。
        """
        batch = self.find_handoff_batch(batch_id)
        if batch is None:
            return {"ok": False, "error": f"交接批次 {batch_id} 不存在"}
        part = batch["partitions"].get(partition)
        if part is None:
            return {"ok": False, "error": f"批次 {batch_id} 不含分区 {partition}"}
        if batch["status"] == BATCH_CLOSED:
            return {"ok": False, "error": f"交接批次 {batch_id} 已闭环，不再接收回执"}

        attempt = {"receipt_no": receipt_no, "checksum": checksum, "at": _now(),
                   "readable": readable, "fail": fail, "reason": reason}
        part["receipt_attempts"].append(attempt)

        # 幂等台账：同一回执单号在任何分区上只生效一次
        ledger_state = self._load_state(STATE_LEDGER)
        ledger = ledger_state.setdefault("ledger", {})
        if receipt_no in ledger:
            self._save_state(STATE_BATCHES, self._load_state(STATE_BATCHES))
            store.log_change(
                self.MODULE, batch.get("archive_task_id") or 0, "归档回执重复提交",
                before=part["status"], after=part["status"],
                source="handoff", scope=f"cold_archive:{batch_id}:{partition}",
            )
            return {
                "ok": True, "accepted": False,
                "batch_id": batch_id, "partition": partition,
                "receipt_no": receipt_no,
                "effect": part["status"],
                "reason": "重复回执已忽略，只生效一次",
                "first_seen": dict(ledger[receipt_no]),
            }

        expected = part["expected_rows"]
        rows = int(confirmed_rows if confirmed_rows is not None else expected)
        interrupted = bool(fail) or not readable or rows != expected

        if interrupted:
            before = part["status"]
            part["status"] = P_INTERRUPTED
            part["reclamation"] = R_BLOCKED
            part["readable_copy"] = False
            part["interrupt_reason"] = reason or "冷端回执中断或副本不可读"
            ledger[receipt_no] = {
                "ok": True, "accepted": True, "batch_id": batch_id, "partition": partition,
                "effect": P_INTERRUPTED, "at": _now(),
            }
            self._save_state(STATE_LEDGER, ledger_state)
            self._save_state(STATE_BATCHES, self._load_state(STATE_BATCHES))
            policy_view = self._advance_policy_reclamation(batch)
            store.log_change(
                self.MODULE, batch.get("archive_task_id") or 0, "归档回执中断",
                before=before, after=P_INTERRUPTED,
                source="handoff", scope=f"cold_archive:{batch_id}:{partition}",
            )
            self.emit_event("archive_receipt_interrupted", {
                "batch_id": batch_id, "partition": partition, "receipt_no": receipt_no,
                "reason": part["interrupt_reason"],
            })
            return {"ok": True, "accepted": True, "batch_id": batch_id,
                    "partition": partition, "effect": P_INTERRUPTED,
                    "reason": part["interrupt_reason"], "policy": policy_view}

        # 成功确认：登记校验号与冷端实际存储位置
        before = part["status"]
        part["status"] = P_CONFIRMED
        part["receipt_no"] = receipt_no
        part["checksum"] = checksum
        part["confirmed_rows"] = rows
        part["confirmed_at"] = _now()
        part["readable_copy"] = True
        part["locations"]["cold"] = (
            f"{batch['cold_storage_base'].rstrip('/')}/{batch_id}/{partition}"
        )
        # 已确认的完整冷副本具备清退前提；热端是否释放仍以 release 为准
        if part["hot_state"] != H_RELEASED:
            part["reclamation"] = R_WAITING
        part.pop("interrupt_reason", None)

        ledger[receipt_no] = {
            "ok": True, "accepted": True, "batch_id": batch_id, "partition": partition,
            "effect": P_CONFIRMED, "checksum": checksum, "at": _now(),
        }
        self._save_state(STATE_LEDGER, ledger_state)

        # 全部分区确认后闭环批次
        if all(p["status"] == P_CONFIRMED for p in batch["partitions"].values()):
            batch["status"] = BATCH_CLOSED
            batch["closed_at"] = _now()
        self._save_state(STATE_BATCHES, self._load_state(STATE_BATCHES))

        # 按交接批次把清退推进状态（待清退）回写生命周期策略
        policy_view = self._advance_policy_reclamation(batch)

        store.log_change(
            self.MODULE, batch.get("archive_task_id") or 0, "归档回执确认",
            before=before, after=P_CONFIRMED,
            source="handoff", scope=f"cold_archive:{batch_id}:{partition}:{checksum}",
        )
        self.emit_event("archive_receipt_confirmed", {
            "batch_id": batch_id, "partition": partition,
            "receipt_no": receipt_no, "checksum": checksum, "rows": rows,
        })
        return {"ok": True, "accepted": True, "batch_id": batch_id,
                "partition": partition, "effect": P_CONFIRMED,
                "checksum": checksum, "confirmed_rows": rows,
                "batch_status": batch["status"], "policy": policy_view}

    # ── 热端启用申请（与归档并行，不互斥） ──────────────────────

    def request_hot_enable(
        self, partition: str, *, asset_code: str | None = None,
        reader: str = "分析员", reason: str = "读取分区数据",
    ) -> dict[str, Any]:
        """热端启用申请：即使归档回执尚未返回，热端数据在释放前仍可读。

        返回归档冲突标记与当前读取方案，分析员据此知道冷副本能否用于随后恢复。
        """
        batch = self._find_partition_batch(partition, asset_code)
        if batch is None:
            return {"ok": False, "error": f"分区 {partition} 没有交接批次"}
        part = batch["partitions"][partition]
        asset = batch["asset_code"]

        hot_state = self._load_state(STATE_HOT)
        reg = hot_state.setdefault("partitions", {})
        key = f"{asset}/{partition}"
        entry = reg.setdefault(key, {
            "asset_code": asset, "partition": partition,
            "batch_id": batch["batch_id"], "enabled": False, "hot_rows": 0,
            "enabled_by": None, "enabled_at": None, "requests": [],
        })
        request_log = {"reader": reader, "reason": reason, "at": _now()}
        entry["requests"].append(request_log)

        archive_conflict = part["status"] != P_CONFIRMED
        if part["hot_state"] == H_RELEASED:
            # 热端已释放：启用申请只能走冷端恢复入口
            entry["enabled"] = False
            self._save_state(STATE_HOT, hot_state)
            recovery = self.verify_recovery_copy(batch["batch_id"], partition)
            return {
                "ok": True, "granted": False, "partition": partition,
                "asset_code": asset, "batch_id": batch["batch_id"],
                "archive_conflict": archive_conflict,
                "message": "热端已释放，启用申请转入冷端恢复入口",
                "read_plan": self.read_partition(partition, asset_code=asset),
                "recovery_copy": recovery,
            }

        # 热端数据在释放前始终保留，启用申请直接通过
        entry["enabled"] = True
        entry["enabled_by"] = reader
        entry["enabled_at"] = _now()
        entry["hot_rows"] = part["expected_rows"]
        part["hot_state"] = H_ENABLED
        self._save_state(STATE_HOT, hot_state)
        self._save_state(STATE_BATCHES, self._load_state(STATE_BATCHES))

        recovery = self.verify_recovery_copy(batch["batch_id"], partition)
        store.log_change(
            self.MODULE, batch.get("archive_task_id") or 0, "热端启用申请",
            before=H_RETAINED, after=H_ENABLED,
            source="handoff", scope=f"cold_archive:{batch['batch_id']}:{partition}",
        )
        self.emit_event("hot_enable_requested", {
            "batch_id": batch["batch_id"], "partition": partition,
            "archive_conflict": archive_conflict, "reader": reader,
        })
        return {
            "ok": True, "granted": True, "partition": partition,
            "asset_code": asset, "batch_id": batch["batch_id"],
            "hot_state": H_ENABLED,
            "archive_conflict": archive_conflict,
            "conflict_reason": (
                "归档回执尚未确认，热端数据继续保留可读，冷副本暂不可用于恢复"
                if archive_conflict else "冷副本已确认，热端与冷端均可读"
            ),
            "hot_readable_rows": entry["hot_rows"],
            "recovery_copy": recovery,
            "read_plan": self.read_partition(partition, asset_code=asset),
        }

    # ── 热端释放：释放前强制校验完整可读冷副本 ──────────────────

    def release_hot_partition(self, batch_id: str, partition: str) -> dict[str, Any]:
        """释放热端分区（清退）。

        前置条件：存在一份行数完整、校验号已确认且可读的冷副本。
        未确认 / 中断 / 行数不符的分区一律拒绝释放，东区确认不能释放西区。
        """
        batch = self.find_handoff_batch(batch_id)
        if batch is None:
            return {"ok": False, "error": f"交接批次 {batch_id} 不存在"}
        part = batch["partitions"].get(partition)
        if part is None:
            return {"ok": False, "error": f"批次 {batch_id} 不含分区 {partition}"}

        guard = self._guard_complete_copy(part)
        if not guard["passed"]:
            store.log_change(
                self.MODULE, batch.get("archive_task_id") or 0, "热端释放拒绝",
                before=part["hot_state"], after=part["hot_state"],
                source="handoff",
                scope=f"cold_archive:{batch_id}:{partition}:{guard['reason']}",
            )
            return {"ok": False, "error": guard["reason"],
                    "batch_id": batch_id, "partition": partition,
                    "partition_status": part["status"]}

        before = part["hot_state"]
        part["hot_state"] = H_RELEASED
        part["reclamation"] = R_DONE

        hot_state = self._load_state(STATE_HOT)
        entry = hot_state.setdefault("partitions", {}).get(f"{batch['asset_code']}/{partition}")
        if entry is not None:
            entry["enabled"] = False
            entry["hot_rows"] = 0
            entry["released_at"] = _now()
        self._save_state(STATE_HOT, hot_state)
        self._save_state(STATE_BATCHES, self._load_state(STATE_BATCHES))

        # 沿调用链把清退状态按交接批次推进回生命周期策略
        policy_view = self._advance_policy_reclamation(batch)

        store.log_change(
            self.MODULE, batch.get("archive_task_id") or 0, "热端释放",
            before=before, after=H_RELEASED,
            source="pipeline_4",
            scope=f"cold_archive:{batch_id}:{partition}:校验号{part['checksum']}",
        )
        self.emit_event("hot_partition_released", {
            "batch_id": batch_id, "partition": partition,
            "checksum": part["checksum"], "reclamation": R_DONE,
        })
        return {"ok": True, "batch_id": batch_id, "partition": partition,
                "hot_state": H_RELEASED, "reclamation": R_DONE,
                "checksum": part["checksum"], "policy": policy_view}

    def _guard_complete_copy(self, part: dict[str, Any]) -> dict[str, Any]:
        """释放前置校验：必须有一份完整且可读的冷副本。"""
        if part["status"] != P_CONFIRMED:
            return {"passed": False,
                    "reason": f"冷副本未确认（分区状态={part['status']}），禁止释放热端"}
        if not part.get("readable_copy") or not part.get("checksum"):
            return {"passed": False, "reason": "冷副本不可读或缺少校验号，禁止释放热端"}
        if int(part["confirmed_rows"]) != int(part["expected_rows"]):
            return {"passed": False,
                    "reason": f"冷副本行数不完整（{part['confirmed_rows']}/{part['expected_rows']}），禁止释放热端"}
        if not part["locations"].get("cold"):
            return {"passed": False, "reason": "冷端存储位置缺失，禁止释放热端"}
        return {"passed": True, "reason": "冷副本完整可读"}

    def verify_recovery_copy(self, batch_id: str, partition: str) -> dict[str, Any]:
        """核对已有冷副本能否用于随后一次恢复。"""
        batch = self.find_handoff_batch(batch_id)
        if batch is None:
            return {"ok": False, "usable": False, "reason": f"交接批次 {batch_id} 不存在"}
        part = batch["partitions"].get(partition)
        if part is None:
            return {"ok": False, "usable": False, "reason": f"批次不含分区 {partition}"}
        guard = self._guard_complete_copy(part)
        return {
            "ok": True,
            "usable": guard["passed"],
            "reason": guard["reason"],
            "batch_id": batch_id,
            "partition": partition,
            "partition_status": part["status"],
            "checksum": part.get("checksum"),
            "receipt_no": part.get("receipt_no"),
            "cold_location": part["locations"].get("cold"),
            "recovery_entry": part.get("recovery_entry"),
            "expected_rows": part["expected_rows"],
            "confirmed_rows": part["confirmed_rows"],
        }

    # ── 清退状态推进回生命周期策略（按交接批次） ────────────────

    def _advance_policy_reclamation(self, batch: dict[str, Any]) -> dict[str, Any]:
        policies = store.find_by_field(
            "lifecycle_policy", "目标资产", batch["asset_code"]
        )
        policy = None
        if batch.get("policy_id") is not None:
            policy = store.find("lifecycle_policy", batch["policy_id"])
        if policy is None and policies:
            active = [p for p in policies if p.get("status") in ("已生效", "流转中")]
            policy = active[0] if active else policies[0]
        if policy is None:
            return {"linked": False}

        detail = {
            name: {
                "reclamation": p["reclamation"],
                "hot_state": p["hot_state"],
                "status": p["status"],
                "checksum": p.get("checksum"),
            }
            for name, p in batch["partitions"].items()
        }
        batches = policy.setdefault("handoff_batches", [])
        if batch["batch_id"] not in batches:
            batches.append(batch["batch_id"])
        policy["清退明细"] = detail
        done = all(p["reclamation"] == R_DONE for p in batch["partitions"].values())
        blocked = any(p["reclamation"] == R_BLOCKED for p in batch["partitions"].values())
        if done:
            policy["清退状态"] = "批次已清退"
        elif blocked:
            policy["清退状态"] = "清退阻塞"
        else:
            policy["清退状态"] = "分区清退中"
        policy["last_handoff_batch"] = batch["batch_id"]
        policy["updated_at"] = _now()

        store.log_change(
            "lifecycle_policy", policy["id"], "清退状态推进",
            before="-", after=policy["清退状态"],
            source="pipeline_4",
            scope=f"lifecycle_policy<-cold_archive:{batch['batch_id']}",
        )
        return {"linked": True, "policy_id": policy["id"],
                "清退状态": policy["清退状态"], "detail": detail}

    # ── 兼容读取：逐行说明从哪里读 ──────────────────────────────

    def read_partition(self, partition: str, *, asset_code: str | None = None) -> dict[str, Any]:
        """读取分区，兼容已有存储位置与原恢复入口，逐行给出读取来源。"""
        batch = self._find_partition_batch(partition, asset_code)
        if batch is None:
            return {"ok": False, "error": f"分区 {partition} 没有交接批次"}
        part = batch["partitions"][partition]
        hot_available = part["hot_state"] != H_RELEASED
        rows: list[dict[str, Any]] = []
        for row_no in range(1, part["expected_rows"] + 1):
            if hot_available:
                rows.append({
                    "row_no": row_no,
                    "source": "热端",
                    "location": part["locations"]["hot"],
                    "via": "已有存储位置",
                    "checksum": None,
                })
            else:
                rows.append({
                    "row_no": row_no,
                    "source": "冷端",
                    "location": part["locations"]["cold"],
                    "via": "原恢复入口",
                    "checksum": part.get("checksum"),
                })

        hot_reg = self._hot_registry().get(f"{batch['asset_code']}/{partition}", {})
        return {
            "ok": True,
            "asset_code": batch["asset_code"],
            "batch_id": batch["batch_id"],
            "partition": partition,
            "partition_status": part["status"],
            "hot_state": part["hot_state"],
            "reclamation": part["reclamation"],
            "checksum": part.get("checksum"),
            "receipt_no": part.get("receipt_no"),
            "expected_rows": part["expected_rows"],
            "hot_readable_rows": hot_reg.get("hot_rows", part["expected_rows"] if hot_available else 0),
            "cold_readable_rows": part["confirmed_rows"] if part["status"] == P_CONFIRMED else 0,
            "locations": dict(part["locations"]),
            "recovery_entry": part.get("recovery_entry"),
            "rows": rows,
        }

    # ── 批次查询与核对 ──────────────────────────────────────────

    def list_handoff_batches(self, asset_code: str | None = None) -> list[dict[str, Any]]:
        batches = self._all_batches()
        if asset_code:
            batches = [b for b in batches if b["asset_code"] == asset_code]
        return batches

    def get_handoff_batch(self, batch_id: str) -> dict[str, Any] | None:
        return self.find_handoff_batch(batch_id)

    def batch_reconciliation(self, batch_id: str) -> dict[str, Any]:
        """输出批次核对表：热端可读行与归档分区校验记录逐分区对照。"""
        batch = self.find_handoff_batch(batch_id)
        if batch is None:
            return {"ok": False, "error": f"交接批次 {batch_id} 不存在"}
        hot_reg = self._hot_registry()
        partitions = []
        for name, part in batch["partitions"].items():
            hot_entry = hot_reg.get(f"{batch['asset_code']}/{name}", {})
            partitions.append({
                "partition": name,
                "expected_rows": part["expected_rows"],
                "partition_status": part["status"],
                "receipt_no": part.get("receipt_no"),
                "checksum": part.get("checksum"),
                "confirmed_rows": part["confirmed_rows"],
                "hot_state": part["hot_state"],
                "hot_readable_rows": hot_entry.get("hot_rows", 0),
                "cold_readable_rows": part["confirmed_rows"] if part["status"] == P_CONFIRMED else 0,
                "reclamation": part["reclamation"],
                "cold_location": part["locations"].get("cold"),
                "hot_location": part["locations"].get("hot"),
                "recovery_entry": part.get("recovery_entry"),
                "receipt_attempts": len(part.get("receipt_attempts", [])),
            })
        return {
            "ok": True,
            "batch_id": batch_id,
            "asset_code": batch["asset_code"],
            "batch_status": batch["status"],
            "archive_task_id": batch.get("archive_task_id"),
            "opened_at": batch.get("opened_at"),
            "closed_at": batch.get("closed_at"),
            "partitions": partitions,
            "total_expected_rows": sum(p["expected_rows"] for p in partitions),
            "total_hot_readable_rows": sum(p["hot_readable_rows"] for p in partitions),
            "total_cold_confirmed_rows": sum(p["cold_readable_rows"] for p in partitions),
        }
