import json
import math

import pytest

from qzcli import task_dimensions


@pytest.mark.parametrize(
    "value",
    [
        "Infinity",
        "-Infinity",
        "NaN",
        float("inf"),
        float("-inf"),
        float("nan"),
    ],
)
def test_as_float_replaces_non_finite_values(value):
    result = task_dimensions._as_float(value)

    assert result == 0.0
    assert math.isfinite(result)


def test_flatten_task_dimension_produces_strict_json_for_non_finite_metrics():
    task = {
        "cpu": {"total": "Infinity", "used": float("nan"), "usage_rate": "Infinity"},
        "memory": {"total": float("-inf"), "usage_rate": "NaN"},
        "gpu": {"total": "NaN", "usage_rate": float("inf")},
    }

    row = task_dimensions._flatten_task_dimension(task, "ws-1", "Workspace")
    encoded = task_dimensions._json_bytes(row)

    assert json.loads(encoded) == row
    assert row["cpu_usage_rate_pct"] == 0.0
    assert row["memory_usage_rate_pct"] == 0.0
    assert row["gpu_usage_rate_pct"] == 0.0


def test_json_bytes_rejects_non_finite_values_that_bypass_normalization():
    with pytest.raises(ValueError, match="Out of range float values"):
        task_dimensions._json_bytes({"rate_pct": float("inf")})


def test_dashboard_api_requests_preserve_reverse_proxy_prefix():
    html = task_dimensions.HTML_PAGE

    assert "function apiUrl(path, params = null)" in html
    assert "base.pathname = `${base.pathname}/`" in html
    assert 'fetch(apiUrl("api/stop")' in html
    assert "fetch(apiUrl(path, params)" in html
    assert 'fetch("/api/' not in html
    assert '"/api/refresh"' not in html
    assert '"/api/tasks"' not in html


def test_dashboard_reports_non_json_responses_before_parsing():
    html = task_dimensions.HTML_PAGE

    assert "async function readJsonResponse(response)" in html
    assert "const raw = await response.text()" in html
    assert "returned non-JSON" in html
    assert "await response.json()" not in html


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("job_queuing", "queueing"),
        ("QUEUED", "queueing"),
        ("CREATING", "creating"),
        ("job_running", "running"),
        ("SUCCEEDED_RETAINING", "succeeded_retaining"),
        ("job_failed_retaining", "failed_retaining"),
        ("job_failed", "other"),
    ],
)
def test_status_filter_key_normalizes_platform_variants(raw, expected):
    assert task_dimensions._status_filter_key(raw) == expected


def test_flatten_train_job_preserves_requested_resources_for_queued_job():
    row = task_dimensions._flatten_train_job(
        {
            "job_id": "job-q",
            "name": "queued",
            "status": "job_queuing",
            "created_at": "1700000000000",
            "priority_name": "3",
            "gpu_count": 8,
            "node_count": 0,
            "project_id": "p-1",
            "project_name": "Project",
            "logic_compute_group_id": "lcg-1",
            "created_by": {"id": "u-1", "name": "User"},
            "framework_config": [
                {"instance_count": 2, "gpu_count": 4, "cpu": 20, "mem_gi": 200}
            ],
        },
        "ws-1",
        "Workspace",
    )

    assert row["status_group"] == "queueing"
    assert row["status_label"] == "排队中"
    assert row["node_count"] == 2
    assert row["gpu_total"] == 8
    assert row["cpu_total"] == 40
    assert row["memory_total"] == 400
    assert row["created_at_epoch"] == 1700000000000


def test_fetch_task_dimensions_merges_supplemental_queue_rows(monkeypatch):
    class FakeAPI:
        def __init__(self):
            self.status_lists = []

        def list_task_dimension(self, *_args, **_kwargs):
            return {
                "task_dimensions": [
                    {"id": "job-running", "status": "RUNNING", "name": "active"}
                ],
                "total": 1,
            }

        def list_jobs_with_cookie(self, *_args, **kwargs):
            self.status_lists.append(kwargs.get("status_list"))
            return {
                "jobs": [
                    {
                        "job_id": "job-queued",
                        "status": "job_queuing",
                        "name": "queued",
                    }
                ],
                "total": 1,
            }

    api = FakeAPI()
    monkeypatch.setattr(task_dimensions, "get_api", lambda: api)
    monkeypatch.setattr(
        task_dimensions,
        "_authenticated_cookie_context",
        lambda _api: ("cookie", "ws-1"),
    )

    result = task_dimensions._fetch_task_dimensions("ws-1", "Workspace")

    assert {row["id"] for row in result["rows"]} == {
        "job-running",
        "job-queued",
    }
    assert api.status_lists == [task_dimensions.SUPPLEMENTAL_TRAIN_JOB_STATUSES]


def test_dashboard_has_composable_status_filters_and_queue_only_action():
    html = task_dimensions.HTML_PAGE

    assert 'id="statusFilterChips"' in html
    assert 'id="queueOnlyButton"' in html
    assert 'state.selectedStatusFilters = ["queueing"]' in html
    assert "default_status_filters" in html
    assert "selectedStatusFilters.includes(row.status_group" in html
