"""归档交接批次路由：冷热移交确认、热端启用/释放与兼容读取。

路由层不做业务判断，全部判断收敛在 ColdArchiveService / LifecyclePolicyService。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel, Field

from app.services.cold_archive import ColdArchiveService
from app.services.lifecycle_policy import LifecyclePolicyService

router = APIRouter(prefix="/api/handoff", tags=["归档交接批次"])


class OpenBatchPayload(BaseModel):
    asset_code: str
    partitions: dict[str, int] = Field(default_factory=dict)
    archive_task_id: int | None = None
    policy_id: int | None = None


class ReceiptPayload(BaseModel):
    batch_id: str
    partition: str
    receipt_no: str
    checksum: str
    confirmed_rows: int | None = None
    readable: bool = True
    fail: bool = False
    reason: str = ""


class HotEnablePayload(BaseModel):
    partition: str
    asset_code: str | None = None
    reader: str = "分析员"
    reason: str = "读取分区数据"


class ReleasePayload(BaseModel):
    batch_id: str
    partition: str


class ReadPayload(BaseModel):
    partition: str
    asset_code: str | None = None


class PolicyHandoffPayload(BaseModel):
    policy_id: int
    partitions: dict[str, int] = Field(default_factory=dict)


@router.post("/batches")
def open_batch(payload: OpenBatchPayload) -> dict[str, Any]:
    return ColdArchiveService().open_handoff_batch(
        payload.asset_code, payload.partitions,
        archive_task_id=payload.archive_task_id,
        policy_id=payload.policy_id,
    )


@router.get("/batches")
def list_batches(asset_code: str | None = None) -> dict[str, Any]:
    service = ColdArchiveService()
    batches = service.list_handoff_batches(asset_code)
    return {"items": batches, "total": len(batches)}


@router.get("/batches/{batch_id}")
def get_batch(batch_id: str) -> dict[str, Any]:
    batch = ColdArchiveService().get_handoff_batch(batch_id)
    if batch is None:
        return {"ok": False, "error": f"交接批次 {batch_id} 不存在"}
    return batch


@router.get("/batches/{batch_id}/reconciliation")
def reconcile_batch(batch_id: str) -> dict[str, Any]:
    return ColdArchiveService().batch_reconciliation(batch_id)


@router.post("/receipts")
def submit_receipt(payload: ReceiptPayload) -> dict[str, Any]:
    return ColdArchiveService().submit_archive_receipt(
        payload.batch_id, payload.partition, payload.receipt_no, payload.checksum,
        confirmed_rows=payload.confirmed_rows,
        readable=payload.readable, fail=payload.fail, reason=payload.reason,
    )


@router.post("/hot/enable")
def hot_enable(payload: HotEnablePayload) -> dict[str, Any]:
    return ColdArchiveService().request_hot_enable(
        payload.partition, asset_code=payload.asset_code,
        reader=payload.reader, reason=payload.reason,
    )


@router.post("/hot/release")
def hot_release(payload: ReleasePayload) -> dict[str, Any]:
    return ColdArchiveService().release_hot_partition(payload.batch_id, payload.partition)


@router.post("/read")
def read_partition(payload: ReadPayload) -> dict[str, Any]:
    return ColdArchiveService().read_partition(
        payload.partition, asset_code=payload.asset_code,
    )


@router.get("/batches/{batch_id}/partitions/{partition}/recovery")
def recovery_check(batch_id: str, partition: str) -> dict[str, Any]:
    return ColdArchiveService().verify_recovery_copy(batch_id, partition)


@router.post("/policy/open")
def open_by_policy(payload: PolicyHandoffPayload) -> dict[str, Any]:
    return LifecyclePolicyService().open_archive_handoff(
        payload.policy_id, payload.partitions,
    )
