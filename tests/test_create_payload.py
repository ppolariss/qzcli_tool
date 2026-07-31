"""Outbound-payload tests for cmd_create.

Verifies the payload sent to /api/v1/train_job/create no longer contains the
deprecated framework_config[0].spec_id field, and instead nests a
resource_spec_price object alongside image/instance_count/shm_gi.
"""

import argparse
import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import MagicMock
from unittest import mock

from qzcli import cli


class _FakeAPI:
    """Minimal QzAPI stand-in. Captures whatever payload create_job receives."""

    def __init__(self):
        self.last_payload = None

    def create_job(self, payload):
        self.last_payload = payload
        return {"job_id": "job-fake-1", "workspace_id": "ws-test"}

    def create_job_with_cookie(self, cookie, payload):
        self.last_payload = payload
        return {"job_id": "job-fake-1", "workspace_id": "ws-test"}

    def create_job_v2(self, cookie, payload):
        # cmd_create 现在主走 v2 Console API；捕获同一个 payload。
        self.last_payload = payload
        return {"job_id": "job-fake-1", "workspace_id": "ws-test"}

    def list_specs(self, compute_group_id):
        return []


def _build_args(**overrides):
    args = argparse.Namespace(
        interactive=False,
        name="claude-test",
        cmd_str="echo hi",
        workspace="ws-test",
        project="project-test",
        compute_group="lcg-test",
        spec="spec-test",
        image=None,
        image_type=None,
        instances=None,
        shm=None,
        priority=None,
        framework=None,
        exclude_node=None,
        include_node=None,
        no_track=True,
        dry_run=False,
        output_json=True,
    )
    for k, v in overrides.items():
        setattr(args, k, v)
    return args


_FAKE_RESOURCES = {
    "ws-test": {
        "id": "ws-test",
        "name": "test-ws",
        "projects": {"project-test": {"id": "project-test", "name": "p"}},
        "compute_groups": {
            "lcg-test": {"id": "lcg-test", "name": "cg", "gpu_type": "H100"}
        },
        "specs": {
            "spec-test": {
                "id": "spec-test",
                "name": "h100-1g",
                "logic_compute_group_id": "lcg-test",
                "logic_compute_group_ids": ["lcg-test"],
                "gpu_count": 1,
                "cpu_count": 28,
                "memory_gb": 240,
                "gpu_type": "NVIDIA_H100_SXM_80G",
                "gpu_type_display": "H100",
            }
        },
    }
}


class CreatePayloadTests(unittest.TestCase):
    def _run_create(self, args=None):
        api = _FakeAPI()
        if args is None:
            args = _build_args()

        # Patch the singletons cmd_create reaches for, plus the resource-cache
        # accessors it uses to resolve names → ids. We feed everything from
        # _FAKE_RESOURCES so no network/disk hits the real ~/.qzcli files.
        patches = [
            mock.patch("qzcli.cli.get_api", return_value=api),
            mock.patch(
                "qzcli.cli.get_store",
                return_value=mock.MagicMock(add_job=lambda *_: None),
            ),
            mock.patch(
                "qzcli.cli.get_workspace_resources",
                side_effect=lambda ws_id: _FAKE_RESOURCES.get(ws_id),
            ),
            mock.patch("qzcli.cli.find_workspace_by_name", return_value="ws-test"),
            # Force cookie auth path so we exercise create_job_with_cookie.
            mock.patch("qzcli.cli.get_cookie", return_value={"cookie": "fake-cookie"}),
            mock.patch(
                "qzcli.cli._auto_select_resource",
                return_value=("project-test", "p"),
            ),
            # cmd_create calls _validate_cached_resource_membership for project
            # and _validate_cached_spec_membership for spec when both look up
            # by name. Force True so resolution succeeds.
            mock.patch(
                "qzcli.cli._validate_cached_resource_membership",
                return_value=True,
            ),
            mock.patch("qzcli.cli._validate_cached_spec_membership", return_value=True),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        rc = cli.cmd_create(args)
        return rc, api

    def test_payload_contains_resource_spec_price_and_no_spec_id(self):
        # Suppress the JSON status line cmd_create prints on success.
        with redirect_stdout(io.StringIO()):
            rc, api = self._run_create()

        self.assertEqual(0, rc)
        self.assertIsNotNone(api.last_payload, "create_job was not called")

        # Serialize and string-search to make sure spec_id is gone EVERYWHERE,
        # including any nested location.
        serialized = json.dumps(api.last_payload)
        self.assertNotIn(
            "spec_id",
            serialized,
            f"Legacy spec_id field leaked into payload: {serialized}",
        )

        fc = api.last_payload["framework_config"][0]
        self.assertIn("resource_spec_price", fc)
        rsp = fc["resource_spec_price"]

        # All 6 fields the platform expects, with values pulled from the spec cache.
        self.assertEqual(
            {
                "cpu_type": "",
                "cpu_count": 28,
                "gpu_type": "NVIDIA_H100_SXM_80G",
                "gpu_count": 1,
                "memory_size_gib": 240,
                "logic_compute_group_id": "lcg-test",
                "quota_id": "spec-test",
            },
            rsp,
        )

        # framework_config sibling keys still carry image/instance/shm.
        self.assertEqual(
            cli.DEFAULT_CREATE_IMAGE,
            fc["image"],
        )
        self.assertEqual(1, fc["instance_count"])
        self.assertEqual(cli.DEFAULT_CREATE_SHM, fc["shm_gi"])

        # Platform also requires cpu/mem_gi/gpu_count at framework_config[0]
        # alongside resource_spec_price; without these the platform returns
        # "Cpu and Mem can't be empty." (verified empirically 2026-05-06).
        self.assertEqual(28, fc["cpu"])
        self.assertEqual(240, fc["mem_gi"])
        self.assertEqual(1, fc["gpu_count"])

    # ---- exclude_nodes (碎卡治理，v2 顶层选项) ----

    def test_no_exclude_node_absent_from_payload(self):
        with redirect_stdout(io.StringIO()):
            rc, api = self._run_create(_build_args())
        self.assertEqual(0, rc)
        self.assertNotIn("exclude_nodes", api.last_payload)

    def test_exclude_nodes_dedup_and_strip(self):
        args = _build_args(exclude_node=["  gpu-a ", "gpu-b", "gpu-a"])
        with redirect_stdout(io.StringIO()):
            rc, api = self._run_create(args)
        self.assertEqual(0, rc)
        # 顶层、去重、strip、保序
        self.assertEqual(api.last_payload["exclude_nodes"], ["gpu-a", "gpu-b"])

    def test_exclude_empty_name_rejected(self):
        args = _build_args(exclude_node=["gpu-a", "   "])
        with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            rc, api = self._run_create(args)
        self.assertEqual(1, rc)  # 空节点名报错返回 1
        self.assertIsNone(api.last_payload)  # 未提交

    def test_include_nodes_to_specified_nodes(self):
        # --include-node → payload 顶层 specified_nodes(去重/strip);无 exclude_nodes
        args = _build_args(include_node=["  n1 ", "n2", "n1"])
        with redirect_stdout(io.StringIO()):
            rc, api = self._run_create(args)
        self.assertEqual(0, rc)
        self.assertEqual(api.last_payload["specified_nodes"], ["n1", "n2"])
        self.assertNotIn("exclude_nodes", api.last_payload)

    def test_no_include_absent(self):
        with redirect_stdout(io.StringIO()):
            rc, api = self._run_create(_build_args())
        self.assertNotIn("specified_nodes", api.last_payload)

    def _run_counting_routes(self, args):
        """跑 cmd_create，返回 {'v1':n,'v2':n} 路由计数。"""
        api = _FakeAPI()
        calls = {"v1": 0, "v2": 0}
        _v1 = api.create_job_with_cookie
        _v2 = api.create_job_v2
        api.create_job_with_cookie = lambda c, p: calls.__setitem__(
            "v1", calls["v1"] + 1
        ) or _v1(c, p)
        api.create_job_v2 = lambda c, p: calls.__setitem__(
            "v2", calls["v2"] + 1
        ) or _v2(c, p)
        patches = [
            mock.patch("qzcli.cli.get_api", return_value=api),
            mock.patch(
                "qzcli.cli.get_store",
                return_value=mock.MagicMock(add_job=lambda *_: None),
            ),
            mock.patch(
                "qzcli.cli.get_workspace_resources",
                side_effect=lambda w: _FAKE_RESOURCES.get(w),
            ),
            mock.patch("qzcli.cli.find_workspace_by_name", return_value="ws-test"),
            mock.patch("qzcli.cli.get_cookie", return_value={"cookie": "fake"}),
            mock.patch(
                "qzcli.cli._auto_select_resource", return_value=("project-test", "p")
            ),
            mock.patch(
                "qzcli.cli._validate_cached_resource_membership", return_value=True
            ),
            mock.patch("qzcli.cli._validate_cached_spec_membership", return_value=True),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        with redirect_stdout(io.StringIO()):
            rc = cli.cmd_create(args)
        self.assertEqual(0, rc)
        return calls

    def test_plain_create_routes_v2(self):
        # 迁 v2:普通 create 走 v2 Console API(已真机验证)。
        calls = self._run_counting_routes(_build_args())
        self.assertEqual((calls["v1"], calls["v2"]), (0, 1))

    def test_exclude_node_routes_v2(self):
        # 带 --exclude-node 也走 v2（exclude_nodes 是 v2 顶层选项）。
        calls = self._run_counting_routes(_build_args(exclude_node=["gpu-x"]))
        self.assertEqual((calls["v1"], calls["v2"]), (0, 1))


if __name__ == "__main__":
    unittest.main()


class ComputeGroupStaleCacheTests(unittest.TestCase):
    """计算组不在本地缓存时，不能拿过期缓存直接拒。

    真实故障：新建的计算组（`lcg-6eea9db9` MOVA2.0纯交付分区）**真实存在、
    正跑着千卡任务**，但因为还没进本地缓存，create 报
    「计算组 ... 不属于当前工作空间」—— 这句话本身是错的，而且提示去
    `res -u` 也未必解决。缓存总会过期，新建的组必然有这个窗口期。

    正解：缓存说「没有」时跟平台再确认一次（`workspace ListLogicComputeGroups`
    是权威来源、不依赖缓存），确认存在就放行。
    """

    def _api(self, groups=None, fail=False):
        from qzcli.api import QzAPIError

        api = MagicMock()

        def fake_v2(service, action, body, **kw):
            if fail:
                raise QzAPIError("boom")
            return {
                "logic_compute_groups": [
                    {"logic_compute_group_id": g} for g in (groups or [])
                ]
            }

        api._request_v2 = fake_v2
        return api

    def test_group_on_platform_is_accepted(self):
        """缓存里没有、但平台确认存在 → 放行。"""
        from qzcli.cli import _compute_group_exists_on_platform

        got = _compute_group_exists_on_platform(
            self._api(groups=["lcg-real"]), "ws-1", "lcg-real"
        )
        self.assertIs(got, True)

    def test_group_absent_on_platform_is_rejected(self):
        """平台也说没有 → 确实该拒，别修成放行一切。"""
        from qzcli.cli import _compute_group_exists_on_platform

        got = _compute_group_exists_on_platform(
            self._api(groups=["lcg-other"]), "ws-1", "lcg-fake"
        )
        self.assertIs(got, False)

    def test_query_failure_is_inconclusive_not_rejection(self):
        """查不了平台时返回 None（不确定）→ 上层放行让平台自己拒，
        总好过拿过期缓存误伤一个真实存在的计算组。"""
        from qzcli.cli import _compute_group_exists_on_platform

        got = _compute_group_exists_on_platform(self._api(fail=True), "ws-1", "lcg-x")
        self.assertIsNone(got)

    def test_empty_platform_list_is_inconclusive(self):
        """平台返回空列表可能是分页/权限问题，不能据此判定「不存在」。"""
        from qzcli.cli import _compute_group_exists_on_platform

        got = _compute_group_exists_on_platform(self._api(groups=[]), "ws-1", "lcg-x")
        self.assertIsNone(got)


class ProjectStaleCacheTests(unittest.TestCase):
    """项目不在本地缓存时，不能拿过期缓存直接拒。

    与 `ComputeGroupStaleCacheTests` **完全同构** —— cli.py 里项目和计算组的归属
    校验是同一套逻辑，v0.4.2 只给计算组加了平台复核，项目这条漏了。

    真实复现：`project-7e0957fb`（CI-扩散音视频生成）是真实项目，今天的千卡训练
    和推理任务都跑在它下面；把它从缓存删掉，create 就报「项目 ... 不属于当前
    工作空间」—— 这句话是假的。新建/新加入的项目必然有这个窗口期。
    """

    def _api(self, projects_spaces=None, fail=False):
        """projects_spaces: {project_id: [workspace_id, ...]}"""
        from qzcli.api import QzAPIError

        api = MagicMock()
        if fail:
            api.list_projects_raw = MagicMock(side_effect=QzAPIError("boom"))
        else:
            api.list_projects_raw = MagicMock(
                return_value=[
                    {"id": pid, "space_list": [{"id": w} for w in wss]}
                    for pid, wss in (projects_spaces or {}).items()
                ]
            )
        return api

    def test_project_on_platform_is_accepted(self):
        """缓存里没有、但平台确认它属于这个工作空间 → 放行。"""
        from qzcli.cli import _project_belongs_to_workspace_on_platform

        got = _project_belongs_to_workspace_on_platform(
            self._api({"proj-real": ["ws-1", "ws-2"]}), "ws-1", "proj-real"
        )
        self.assertIs(got, True)

    def test_project_in_other_workspace_only_is_rejected(self):
        """项目存在但属于**别的**工作空间 → 确实该拒。"""
        from qzcli.cli import _project_belongs_to_workspace_on_platform

        got = _project_belongs_to_workspace_on_platform(
            self._api({"proj-real": ["ws-other"]}), "ws-1", "proj-real"
        )
        self.assertIs(got, False)

    def test_unknown_project_is_rejected(self):
        from qzcli.cli import _project_belongs_to_workspace_on_platform

        got = _project_belongs_to_workspace_on_platform(
            self._api({"proj-a": ["ws-1"]}), "ws-1", "proj-nonexistent"
        )
        self.assertIs(got, False)

    def test_query_failure_is_inconclusive_not_rejection(self):
        """查不了平台就返回 None（不确定）→ 上层放行让平台自己拒，
        总好过拿过期缓存误伤一个真实项目。"""
        from qzcli.cli import _project_belongs_to_workspace_on_platform

        got = _project_belongs_to_workspace_on_platform(
            self._api(fail=True), "ws-1", "proj-x"
        )
        self.assertIsNone(got)

    def test_empty_project_list_is_inconclusive(self):
        """返回空列表可能是分页/权限问题，不能据此判定「不属于」。"""
        from qzcli.cli import _project_belongs_to_workspace_on_platform

        got = _project_belongs_to_workspace_on_platform(self._api({}), "ws-1", "proj-x")
        self.assertIsNone(got)
