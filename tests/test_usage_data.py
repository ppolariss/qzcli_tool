"""Tests for the shared usage/dashboard data layer in `qzcli.cli`.

Covers `fetch_all_task_dimensions`, `build_node_to_lcg_map`, and
`task_dimension_to_row` — the pure functions that `cmd_usage` and the
`qzcli dashboard` Streamlit app both build on.
"""

import unittest
from unittest import mock

from qzcli import cli


class _FakeAPI:
    """Stubs the cluster-metric endpoints the data layer calls."""

    def __init__(self, task_pages, node_by_lcg):
        # task_pages: list of page dicts, each {"task_dimensions": [...], "total": N}
        self._task_pages = task_pages
        # node_by_lcg: {lcg_id: [node dicts]}
        self._node_by_lcg = node_by_lcg
        self.task_calls = 0

    def list_task_dimension(self, workspace_id, cookie, page_num=1, page_size=200):
        self.task_calls += 1
        # page_num is 1-indexed
        return self._task_pages[page_num - 1]

    def get_cluster_basic_info(self, workspace_id, cookie):
        return {
            "compute_groups": [
                {
                    "compute_group_name": "cg-1",
                    "logic_compute_groups": [
                        {
                            "logic_compute_group_id": "lcg-h100",
                            "logic_compute_group_name": "cuda12.8版本H100",
                        },
                        {
                            "logic_compute_group_id": "lcg-h200",
                            "logic_compute_group_name": "H200-3号机房",
                        },
                    ],
                },
                # 一个没有 lcg 的隔离池，应被跳过
                {"compute_group_name": "隔离池", "logic_compute_groups": []},
            ]
        }

    def list_node_dimension(
        self,
        workspace_id,
        cookie,
        logic_compute_group_id=None,
        compute_group_id=None,
        page_num=1,
        page_size=100,
    ):
        nodes = self._node_by_lcg.get(logic_compute_group_id, [])
        return {"node_dimensions": nodes, "total": len(nodes)}


def _make_api():
    task_pages = [
        {
            "task_dimensions": [
                {
                    "id": "job-a",
                    "name": "train-a",
                    "type": "distributed_training",
                    "priority": 9,
                    "gpu": {"total": 8, "usage_rate": 0.5},
                    "user": {"name": "梁天一"},
                    "project": {"name": "P1"},
                    "status": "RUNNING",
                    "nodes_occupied": {"count": 1, "nodes": ["gpu001"]},
                    "running_time_ms": "7200000",  # 2h
                    "created_at": "2026-07-01 22:41:32 +0800 CST",
                }
            ],
            "total": 2,
        },
        {
            "task_dimensions": [
                {
                    "id": "job-b",
                    "name": "notebook-b",
                    "type": "interactive_modeling",
                    "priority": 3,
                    "gpu": {"total": 1, "usage_rate": 0.0},
                    "user": {"name": "李四"},
                    "project": {"name": "P2"},
                    "status": "RUNNING",
                    "nodes_occupied": {"count": 0, "nodes": []},  # 无节点 → 排队/未分配
                    "running_time_ms": "0",
                    "created_at": "2026-07-01 10:00:00 +0800 CST",
                }
            ],
            "total": 2,
        },
    ]
    node_by_lcg = {
        "lcg-h100": [
            {"name": "gpu001", "gpu_type": "NVIDIA_H100_SXM_80G", "cluster_name": "c-a"}
        ],
        "lcg-h200": [
            {
                "name": "gpu500",
                "gpu_type": "NVIDIA_H200_SXM_141G",
                "cluster_name": "c-b",
            }
        ],
    }
    return _FakeAPI(task_pages, node_by_lcg)


class FetchAllTaskDimensions(unittest.TestCase):
    def test_paginates_until_total(self):
        api = _make_api()
        tasks = cli.fetch_all_task_dimensions(api, "ws-x", "cookie")
        self.assertEqual(len(tasks), 2)
        self.assertEqual(api.task_calls, 2)  # 两页都拉到
        self.assertEqual([t["id"] for t in tasks], ["job-a", "job-b"])


class BuildNodeToLcgMap(unittest.TestCase):
    def test_maps_nodes_to_lcg_and_short_gpu_type(self):
        api = _make_api()
        node_map = cli.build_node_to_lcg_map(api, "ws-x", "cookie")
        self.assertEqual(set(node_map), {"gpu001", "gpu500"})
        self.assertEqual(node_map["gpu001"]["lcg"], "cuda12.8版本H100")
        self.assertEqual(node_map["gpu001"]["gpu_type"], "H100")
        self.assertEqual(node_map["gpu500"]["lcg"], "H200-3号机房")
        self.assertEqual(node_map["gpu500"]["gpu_type"], "H200")


class TaskDimensionToRow(unittest.TestCase):
    def setUp(self):
        api = _make_api()
        self.node_map = cli.build_node_to_lcg_map(api, "ws-x", "cookie")
        self.tasks = cli.fetch_all_task_dimensions(api, "ws-x", "cookie")

    def test_running_task_attributed_to_compute_group(self):
        row = cli.task_dimension_to_row(self.tasks[0], self.node_map, "ws-x")
        self.assertEqual(row["计算组"], "cuda12.8版本H100")
        self.assertEqual(row["GPU类型"], "H100")
        self.assertEqual(row["任务类型"], "分布式训练")
        self.assertEqual(row["优先级档"], "高优(≥6)")
        self.assertEqual(row["GPU"], 8)
        self.assertEqual(row["GPU利用率"], 50.0)
        self.assertEqual(row["运行时长h"], 2.0)
        self.assertEqual(
            row["job_url"],
            "https://qz.sii.edu.cn/jobs/distributedTrainingDetail/job-a?spaceId=ws-x",
        )

    def test_task_without_node_is_unassigned(self):
        row = cli.task_dimension_to_row(self.tasks[1], self.node_map, "ws-x")
        self.assertEqual(row["计算组"], "排队/未分配")
        self.assertEqual(row["GPU类型"], "")
        self.assertEqual(row["优先级档"], "低优(≤3)")
        self.assertEqual(row["任务类型"], "交互式建模")

    def test_priority_bands(self):
        self.assertEqual(cli._priority_band(10), "高优(≥6)")
        self.assertEqual(cli._priority_band(5), "中优(4-5)")
        self.assertEqual(cli._priority_band(1), "低优(≤3)")

    def test_short_gpu_type(self):
        self.assertEqual(cli._short_gpu_type("NVIDIA_H200_SXM_141G"), "H200")
        self.assertEqual(cli._short_gpu_type("NVIDIA_A100_SXM_80G"), "A100")
        self.assertEqual(cli._short_gpu_type(""), "")


class DashboardCommandDependencyCheck(unittest.TestCase):
    def test_missing_streamlit_prints_hint_and_returns_1(self):
        real_import = __import__

        def fake_import(name, *a, **kw):
            if name == "streamlit":
                raise ImportError("no streamlit")
            return real_import(name, *a, **kw)

        args = mock.Mock(workspace="分布式", port=8520, no_browser=False)
        with mock.patch("builtins.__import__", side_effect=fake_import):
            rc = cli.cmd_dashboard(args)
        self.assertEqual(rc, 1)


if __name__ == "__main__":
    unittest.main()
