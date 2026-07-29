#!/usr/bin/env python3
"""同一份数据，v1 和 v2 各拉一次，逐字段 diff。

迁移里最危险的失败模式**不是报错，是静默返回空**：v2 换了字段名（``data.jobs``
→ ``Result.items``），代码不炸，只是列表永远为空。只看"没报错"不算验证通过，
所以每个端点都要拿真实响应对一遍字段。

用法::

    python3 tools/compare_v1_v2.py --workspace-id ws-xxxx
    python3 tools/compare_v1_v2.py --workspace-id ws-xxxx --only jobs notebooks

**只读**：这个脚本不提交、不停止、不修改任何任务。
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from qzcli import api  # noqa: E402
from qzcli.config import get_cookie  # noqa: E402


def keys_of(obj: Any) -> List[str]:
    if isinstance(obj, dict):
        return sorted(obj.keys())
    return []


def first_item(payload: Dict[str, Any], candidates: Tuple[str, ...]) -> Any:
    """从响应里挑出列表字段的第一个元素，用来比对元素级字段名。"""
    for key in candidates:
        val = payload.get(key)
        if isinstance(val, list) and val:
            return val[0]
    return None


def compare(name: str, v1: Any, v2: Any, list_keys: Tuple[str, ...]) -> bool:
    """打印一组 v1/v2 对比，返回是否"结构等价"。"""
    print(f"\n=== {name}")
    if isinstance(v1, Exception):
        print(f"  v1 ✗ {type(v1).__name__}: {v1}")
    if isinstance(v2, Exception):
        print(f"  v2 ✗ {type(v2).__name__}: {v2}")
    if isinstance(v1, Exception) or isinstance(v2, Exception):
        return False

    k1, k2 = keys_of(v1), keys_of(v2)
    print(f"  顶层 v1={k1}")
    print(f"  顶层 v2={k2}")
    if k1 != k2:
        print(
            f"  ⚠ 顶层字段不同：只在 v1={set(k1) - set(k2)} 只在 v2={set(k2) - set(k1)}"
        )

    i1, i2 = first_item(v1, list_keys), first_item(v2, list_keys)
    if i1 is None and i2 is None:
        print("  （两边列表都为空，无法比对元素字段）")
        return k1 == k2
    if (i1 is None) != (i2 is None):
        print(
            f"  ✗ 一边有数据一边没有：v1={'有' if i1 is not None else '空'} "
            f"v2={'有' if i2 is not None else '空'} —— 这就是静默失败"
        )
        return False

    e1, e2 = set(keys_of(i1)), set(keys_of(i2))
    only1, only2 = e1 - e2, e2 - e1
    print(f"  元素字段 共有 {len(e1 & e2)} 个")
    if only1:
        print(f"  ⚠ 只在 v1: {sorted(only1)}")
    if only2:
        print(f"  ⚠ 只在 v2: {sorted(only2)}")
    ok = k1 == k2 and not only1
    print(f"  {'✓ 可安全替换' if ok else '⚠ 需要在适配层补字段映射'}")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workspace-id", required=True)
    ap.add_argument(
        "--only", nargs="*", help="只跑指定分组：jobs notebooks nodes tasks basic"
    )
    args = ap.parse_args()

    cookie_data = get_cookie()
    cookie = (cookie_data or {}).get("cookie")
    if not cookie:
        print("✗ 没有 cookie —— 先 `qzcli login`", file=sys.stderr)
        return 1

    a = api.get_api()
    ws = args.workspace_id
    results: Dict[str, bool] = {}

    def run(fn):
        try:
            return fn()
        except Exception as exc:  # 两边任一失败都是有效结论
            return exc

    groups = args.only or ["jobs", "notebooks", "nodes", "tasks", "basic"]

    if "jobs" in groups:
        results["train jobs"] = compare(
            "train 任务列表  /api/v1/train_job/list  ↔  train ListJobs",
            run(lambda: a._list_jobs_v1(ws, cookie, page_size=5)),
            run(lambda: a._list_jobs_v2(ws, cookie, page_size=5)),
            ("jobs", "items", "list"),
        )

    if "notebooks" in groups:
        results["notebooks"] = compare(
            "开发机列表  /api/v1/notebook/list  ↔  notebook ListNotebooks",
            run(lambda: a._list_notebooks_v1(ws, cookie, page_size=5)),
            run(lambda: a._list_notebooks_v2(ws, cookie, page_size=5)),
            ("list", "items", "notebooks"),
        )

    if "nodes" in groups:
        results["node dimension"] = compare(
            "节点维度  /api/v1/cluster_metric/list_node_dimension  ↔  workspace ListNodeDimension",
            run(lambda: a._list_node_dimension_v1(ws, cookie, page_size=5)),
            run(lambda: a._list_node_dimension_v2(ws, cookie, page_size=5)),
            ("node_dimensions", "items", "list"),
        )

    if "tasks" in groups:
        results["task dimension"] = compare(
            "任务维度  /api/v1/cluster_metric/list_task_dimension  ↔  workspace ListTaskDimension",
            run(lambda: a._list_task_dimension_v1(ws, cookie, page_size=5)),
            run(lambda: a._list_task_dimension_v2(ws, cookie, page_size=5)),
            ("task_dimensions", "items", "list"),
        )
        now = int(time.time())
        results["overview task metric"] = compare(
            "任务概览  /api/v1/cluster_metric/overview_task_metric  ↔  workspace GetOverviewTaskMetric",
            run(lambda: a._list_workspace_tasks_v1(ws, cookie, now - 86400, now)),
            run(lambda: a._list_workspace_tasks_v2(ws, cookie, now - 86400, now)),
            ("task_groups", "items"),
        )

    if "basic" in groups:
        results["cluster basic info"] = compare(
            "集群基础信息  /api/v1/cluster_metric/cluster_basic_info  ↔  workspace GetBasicInfo",
            run(lambda: a._cluster_basic_info_v1(ws, cookie)),
            run(lambda: a._cluster_basic_info_v2(ws, cookie)),
            ("clusters", "compute_groups"),
        )

    print("\n" + "=" * 60)
    for name, ok in results.items():
        print(f"  {'✓' if ok else '⚠'}  {name}")
    bad = [n for n, ok in results.items() if not ok]
    print(
        f"\n{len(results) - len(bad)}/{len(results)} 结构等价"
        + (f"，需人工确认：{bad}" if bad else "")
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
