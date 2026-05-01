"""Helpers for querying a user's jobs from both platform job views."""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .store import JobRecord


TRAIN_ENDPOINT = "/api/v1/train_job/list"
HPC_ENDPOINT = "/api/v1/hpc_jobs/list"
TRAIN_QUEUE_STATUS_FILTERS = ["job_queuing", "job_pending", "CREATING"]
ACTIVE_TRAIN_STATUS_FILTERS = ["job_queuing", "job_pending", "job_running", "CREATING"]
QUEUE_STATUS_FILTERS = ["QUEUEING", "CREATING"]
ACTIVE_HPC_STATUS_FILTERS = ["QUEUEING", "RUNNING", "CREATING"]


def _created_by_metadata(job_data: Dict[str, Any]) -> Dict[str, str]:
    created_by = job_data.get("created_by") or {}
    if isinstance(created_by, dict):
        return {
            "created_by_id": str(created_by.get("id", "") or ""),
            "created_by_name": str(created_by.get("name", "") or ""),
            "created_by_name_en": str(created_by.get("name_en", "") or ""),
        }
    if created_by:
        return {
            "created_by_id": str(created_by),
            "created_by_name": "",
            "created_by_name_en": "",
        }
    return {
        "created_by_id": "",
        "created_by_name": "",
        "created_by_name_en": "",
    }


def _raw_status(job_data: Dict[str, Any], job: JobRecord) -> str:
    return str(job_data.get("status", job.status) or "")


def _normalize_status_filter(status: str) -> str:
    status_key = str(status or "").upper()
    aliases = {
        "RUNNING": "job_running",
        "PENDING": "job_pending",
        "QUEUEING": "job_queuing",
        "QUEUING": "job_queuing",
        "QUEUED": "job_queuing",
        "STOPPED": "job_stopped",
        "TERMINATED": "job_stopped",
        "FAILED": "job_failed",
        "FAIL": "job_failed",
        "SUCCEEDED": "job_succeeded",
        "SUCCESS": "job_succeeded",
        "COMPLETED": "job_succeeded",
        "FINISHED": "job_succeeded",
    }
    return aliases.get(status_key, str(status or ""))


def _is_waiting_status(raw_status: str, normalized_status: str) -> bool:
    raw_upper = raw_status.upper()
    normalized = normalized_status.lower()
    return (
        normalized in {"job_queuing", "job_queued", "job_pending"}
        or "QUEU" in raw_upper
        or "PENDING" in raw_upper
        or "CREATING" in raw_upper
        or "WAIT" in raw_upper
    )


def _is_active_status(raw_status: str, normalized_status: str) -> bool:
    raw_upper = raw_status.upper()
    normalized = normalized_status.lower()
    return (
        normalized in {"job_running", "job_queuing", "job_queued", "job_pending"}
        or "RUNNING" in raw_upper
        or _is_waiting_status(raw_status, normalized_status)
    )


def _matches_filters(
    job_data: Dict[str, Any],
    job: JobRecord,
    *,
    status: str = "",
    queued_only: bool = False,
    running_only: bool = False,
) -> bool:
    raw_status = _raw_status(job_data, job)
    if status:
        status_filter = status.upper()
        normalized_filter = _normalize_status_filter(status).lower()
        return (
            status_filter in raw_status.upper()
            or status_filter in job.status.upper()
            or normalized_filter == job.status.lower()
        )
    if queued_only:
        return _is_waiting_status(raw_status, job.status)
    if running_only:
        return _is_active_status(raw_status, job.status)
    return True


def _job_from_api_response(
    job_data: Dict[str, Any],
    *,
    workspace_id: str,
    workspace_name: str,
    job_view: str,
    endpoint: str,
) -> JobRecord:
    source = f"api_{job_view}_cookie"
    job = JobRecord.from_api_response(job_data, source=source)
    if not job.job_id:
        job.job_id = str(job_data.get("id", "") or job_data.get("job_id", "") or "")
    if not job.workspace_id:
        job.workspace_id = workspace_id

    metadata = dict(job.metadata or {})
    metadata.update(_created_by_metadata(job_data))
    metadata["workspace_name"] = workspace_name
    metadata["job_view"] = job_view
    metadata["endpoint"] = endpoint
    metadata["status_raw"] = _raw_status(job_data, job)
    job.metadata = metadata
    return job


def _append_unique(records: Dict[Tuple[str, str, str], JobRecord], job: JobRecord) -> None:
    key = (
        str(job.metadata.get("job_view", "")),
        job.workspace_id,
        job.job_id or job.name or repr(job.metadata),
    )
    records[key] = job


def _paginate_train_jobs(
    api: Any,
    workspace_id: str,
    workspace_name: str,
    cookie: str,
    *,
    created_by: str,
    page_size: int,
    max_pages: int,
    status: str = "",
    queued_only: bool = False,
    running_only: bool = False,
) -> Tuple[List[JobRecord], int, List[str]]:
    jobs: List[JobRecord] = []
    warnings: List[str] = []
    raw_total = 0

    if status:
        status_filters: List[str] = [_normalize_status_filter(status)]
    elif queued_only:
        status_filters = list(TRAIN_QUEUE_STATUS_FILTERS)
    elif running_only:
        status_filters = list(ACTIVE_TRAIN_STATUS_FILTERS)
    else:
        status_filters = []

    query_statuses: Iterable[Optional[str]] = status_filters or [None]
    for status_filter in query_statuses:
        page_num = 1
        status_total = 0
        while True:
            try:
                payload = api.list_jobs_with_cookie(
                    workspace_id,
                    cookie,
                    page_num=page_num,
                    page_size=page_size,
                    created_by=created_by,
                    status=status_filter,
                )
            except Exception as exc:
                label = status_filter or "ALL"
                warnings.append(f"{workspace_name or workspace_id} {TRAIN_ENDPOINT} {label}: {exc}")
                break

            if page_num == 1:
                status_total = int(payload.get("total", 0) or 0)
                raw_total += status_total

            page_jobs = payload.get("jobs", []) or []
            for job_data in page_jobs:
                job = _job_from_api_response(
                    job_data,
                    workspace_id=workspace_id,
                    workspace_name=workspace_name,
                    job_view="distributed_training",
                    endpoint=TRAIN_ENDPOINT,
                )
                if _matches_filters(
                    job_data,
                    job,
                    status=status,
                    queued_only=queued_only,
                    running_only=running_only,
                ):
                    jobs.append(job)

            if page_num * page_size >= status_total or not page_jobs:
                break
            if max_pages > 0 and page_num >= max_pages:
                label = status_filter or "ALL"
                warnings.append(
                    f"{workspace_name or workspace_id} {TRAIN_ENDPOINT} {label}: 已达到 max_pages={max_pages}，结果可能被截断"
                )
                break
            page_num += 1

    return jobs, raw_total, warnings


def _paginate_hpc_jobs(
    api: Any,
    workspace_id: str,
    workspace_name: str,
    cookie: str,
    *,
    created_by: str,
    page_size: int,
    max_pages: int,
    status: str = "",
    queued_only: bool = False,
    running_only: bool = False,
) -> Tuple[List[JobRecord], int, List[str]]:
    jobs: List[JobRecord] = []
    warnings: List[str] = []
    raw_total = 0

    if status:
        status_filters: List[str] = [status.upper()]
    elif queued_only:
        status_filters = list(QUEUE_STATUS_FILTERS)
    elif running_only:
        status_filters = list(ACTIVE_HPC_STATUS_FILTERS)
    else:
        status_filters = []

    query_statuses: Iterable[Optional[str]] = status_filters or [None]
    for status_filter in query_statuses:
        page_num = 1
        status_total = 0
        while True:
            try:
                payload = api.list_hpc_jobs_with_cookie(
                    workspace_id,
                    cookie,
                    page_num=page_num,
                    page_size=page_size,
                    created_by=created_by,
                    status=status_filter,
                )
            except Exception as exc:
                label = status_filter or "ALL"
                warnings.append(f"{workspace_name or workspace_id} {HPC_ENDPOINT} {label}: {exc}")
                break

            if page_num == 1:
                status_total = int(payload.get("total", 0) or 0)
                raw_total += status_total

            page_jobs = payload.get("jobs", []) or []
            for job_data in page_jobs:
                job = _job_from_api_response(
                    job_data,
                    workspace_id=workspace_id,
                    workspace_name=workspace_name,
                    job_view="hpc",
                    endpoint=HPC_ENDPOINT,
                )
                if _matches_filters(
                    job_data,
                    job,
                    status=status,
                    queued_only=queued_only,
                    running_only=running_only,
                ):
                    jobs.append(job)

            if page_num * page_size >= status_total or not page_jobs:
                break
            if max_pages > 0 and page_num >= max_pages:
                label = status_filter or "ALL"
                warnings.append(
                    f"{workspace_name or workspace_id} {HPC_ENDPOINT} {label}: 已达到 max_pages={max_pages}，结果可能被截断"
                )
                break
            page_num += 1

    return jobs, raw_total, warnings


def list_user_jobs(
    api: Any,
    cookie: str,
    workspace_refs: List[Dict[str, str]],
    *,
    created_by: str,
    include_train: bool = True,
    include_hpc: bool = True,
    status: str = "",
    queued_only: bool = False,
    running_only: bool = False,
    page_size: int = 100,
    max_pages: int = 5,
) -> Dict[str, Any]:
    """List jobs for one created_by across train_job/list and hpc_jobs/list."""
    if not created_by:
        raise ValueError("created_by is required")

    records: Dict[Tuple[str, str, str], JobRecord] = {}
    warnings: List[str] = []
    raw_totals = {
        "distributed_training": 0,
        "hpc": 0,
    }

    for workspace_ref in workspace_refs:
        workspace_id = workspace_ref["id"]
        workspace_name = workspace_ref.get("name", "")

        if include_train:
            train_jobs, train_total, train_warnings = _paginate_train_jobs(
                api,
                workspace_id,
                workspace_name,
                cookie,
                created_by=created_by,
                page_size=page_size,
                max_pages=max_pages,
                status=status,
                queued_only=queued_only,
                running_only=running_only,
            )
            raw_totals["distributed_training"] += train_total
            warnings.extend(train_warnings)
            for job in train_jobs:
                _append_unique(records, job)

        if include_hpc:
            hpc_jobs, hpc_total, hpc_warnings = _paginate_hpc_jobs(
                api,
                workspace_id,
                workspace_name,
                cookie,
                created_by=created_by,
                page_size=page_size,
                max_pages=max_pages,
                status=status,
                queued_only=queued_only,
                running_only=running_only,
            )
            raw_totals["hpc"] += hpc_total
            warnings.extend(hpc_warnings)
            for job in hpc_jobs:
                _append_unique(records, job)

    jobs = list(records.values())
    jobs.sort(key=lambda job: job.created_at or "", reverse=True)
    return {
        "jobs": jobs,
        "warnings": warnings,
        "raw_totals": raw_totals,
    }


def job_summary(job: JobRecord) -> Dict[str, Any]:
    metadata = job.metadata or {}
    return {
        "job_id": job.job_id,
        "name": job.name,
        "status": job.status,
        "status_raw": metadata.get("status_raw", job.status),
        "workspace_id": job.workspace_id,
        "workspace_name": metadata.get("workspace_name", ""),
        "job_view": metadata.get("job_view", ""),
        "endpoint": metadata.get("endpoint", ""),
        "created_by_id": metadata.get("created_by_id", ""),
        "created_by_name": metadata.get("created_by_name", ""),
        "project_id": job.project_id,
        "project_name": job.project_name,
        "compute_group_name": job.compute_group_name,
        "gpu_type": job.gpu_type,
        "gpu_count": job.gpu_count,
        "instance_count": job.instance_count,
        "priority_level": job.priority_level,
        "running_time_ms": job.running_time_ms,
        "created_at": job.created_at,
        "finished_at": job.finished_at,
        "url": job.url,
    }


def summarize_user_jobs(jobs: List[JobRecord]) -> Dict[str, Dict[str, int]]:
    return {
        "status_counts": dict(Counter(str((job.metadata or {}).get("status_raw", job.status)) for job in jobs)),
        "view_counts": dict(Counter(str((job.metadata or {}).get("job_view", "")) for job in jobs)),
        "workspace_counts": dict(Counter(str((job.metadata or {}).get("workspace_name", job.workspace_id)) for job in jobs)),
    }
