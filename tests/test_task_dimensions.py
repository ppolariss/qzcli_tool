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
