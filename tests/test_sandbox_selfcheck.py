"""沙箱自检 —— 后面所有缓存测试都建在它上面，它自己必须先被验证。

沙箱失效的失败模式很恶心：**测试照常通过**，只是悄悄写到了用户真实的
``~/.qzcli``，直到某次跑完测试发现登录态没了才发现。所以这里逐条钉死。
"""

import json
import os
import sys
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from support import cache_states  # noqa: E402
from support.sandbox import (  # noqa: E402
    config_paths_are_sandboxed,
    real_home,
    sandbox_home,
)

from qzcli import api, cli, config  # noqa: E402


class SandboxIsolationTests(unittest.TestCase):
    def test_all_config_paths_redirected(self):
        """8 个路径常量 + 两份 CONFIG_DIR 拷贝，一个都不能漏。"""
        with sandbox_home() as sandbox_dir:
            ok, offenders = config_paths_are_sandboxed()
            self.assertTrue(ok, f"这些常量仍指向真实 HOME: {offenders}")
            self.assertEqual(config.CONFIG_DIR, sandbox_dir)
            self.assertEqual(config.RESOURCES_FILE, sandbox_dir / "resources.json")

    def test_by_value_import_copies_are_patched(self):
        """cli/api 是 ``from .config import CONFIG_DIR``（按值拷贝）。

        只 patch config.CONFIG_DIR 而漏掉这两份拷贝，是最容易犯且最难发现的错 ——
        cli 仍会往真实 HOME 写 ``.relogin.lock``。
        """
        with sandbox_home() as sandbox_dir:
            self.assertEqual(cli.CONFIG_DIR, sandbox_dir)
            self.assertEqual(api.CONFIG_DIR, sandbox_dir)

    def test_home_env_var_redirected(self):
        """运行期才求值的 ``Path.home()`` 也要落在沙箱里。"""
        with sandbox_home() as sandbox_dir:
            self.assertEqual(Path.home(), sandbox_dir.parent)

    def test_ambient_env_is_cleared(self):
        """跑测试的人 shell 里设了 QZCLI_* 不该影响结果。"""
        os.environ["QZCLI_SESSION_ID"] = "从外面漏进来的"
        try:
            with sandbox_home():
                self.assertIsNone(os.environ.get("QZCLI_SESSION_ID"))
            self.assertEqual(os.environ.get("QZCLI_SESSION_ID"), "从外面漏进来的")
        finally:
            os.environ.pop("QZCLI_SESSION_ID", None)

    def test_state_restored_after_exit(self):
        before = (
            config.CONFIG_DIR,
            config.RESOURCES_FILE,
            cli.CONFIG_DIR,
            api.CONFIG_DIR,
            os.environ.get("HOME"),
        )
        with sandbox_home(resources=cache_states.healthy()):
            pass
        after = (
            config.CONFIG_DIR,
            config.RESOURCES_FILE,
            cli.CONFIG_DIR,
            api.CONFIG_DIR,
            os.environ.get("HOME"),
        )
        self.assertEqual(before, after)

    def test_state_restored_even_on_exception(self):
        """用例抛异常时也必须还原，否则一个坏用例会污染后面所有用例。"""
        before = config.CONFIG_DIR
        with self.assertRaises(RuntimeError):
            with sandbox_home():
                raise RuntimeError("boom")
        self.assertEqual(config.CONFIG_DIR, before)

    def test_temp_dir_removed(self):
        with sandbox_home() as sandbox_dir:
            captured = sandbox_dir
            self.assertTrue(captured.exists())
        self.assertFalse(captured.exists())

    def test_real_config_dir_untouched(self):
        """端到端自检：跑完一轮读写，真实 ``~/.qzcli`` 的内容不变。

        这是计划里点名要的那条 —— 直接比对目录内容，不依赖前面几条的推理。
        """
        real_dir = real_home() / ".qzcli"
        if not real_dir.exists():
            self.skipTest("本机没有 ~/.qzcli，跳过")

        def snapshot():
            out = {}
            for p in sorted(real_dir.rglob("*")):
                if p.is_file():
                    st = p.stat()
                    out[str(p.relative_to(real_dir))] = (st.st_size, st.st_mtime_ns)
            return out

        before = snapshot()
        with sandbox_home(resources=cache_states.healthy(), cookie="fake=1") as sbx:
            (sbx / "resources.json").write_text("{}", encoding="utf-8")
            (sbx / "jobs.json").write_text("[]", encoding="utf-8")
            config.save_cookie("sandbox-cookie")
        self.assertEqual(before, snapshot(), "沙箱漏了：真实 ~/.qzcli 被改动")


class CacheStateFixtureTests(unittest.TestCase):
    def test_every_state_is_writable_and_readable_back(self):
        """每个 fixture 都要能真的落盘并被读回 —— 包括故意损坏的那两个。"""
        for name, (build, desc) in cache_states.ALL_STATES.items():
            with self.subTest(state=name, desc=desc):
                payload = build()
                with sandbox_home(resources=payload) as sandbox_dir:
                    path = sandbox_dir / "resources.json"
                    if payload is None:
                        self.assertFalse(path.exists(), "missing 态不该建文件")
                        continue
                    self.assertTrue(path.exists())
                    raw = path.read_text(encoding="utf-8")
                    if name == "corrupt-json":
                        with self.assertRaises(json.JSONDecodeError):
                            json.loads(raw)
                    else:
                        self.assertEqual(json.loads(raw), payload)

    def test_missing_and_empty_are_distinct(self):
        """ "文件不存在"和"文件是 {}"是两个场景，不能混为一谈。"""
        self.assertIsNone(cache_states.missing())
        self.assertEqual(cache_states.empty(), {})

    def test_fixture_shape_matches_real_cache(self):
        """fixture 的结构必须和真实 ``resources.json`` 一致，否则测的是幻觉。

        真实缓存顶层是 ``{ws_id: ws}`` 扁平映射，且 ``compute_groups`` /
        ``projects`` / ``specs`` 都是**按 id 索引的 dict**（不是 list）—— 这几点
        照文档猜会全猜错。
        """
        data = cache_states.healthy()
        self.assertNotIn("workspaces", data, "顶层没有 workspaces 这层包装")
        ws = data[cache_states.WS_ID]
        for field in ("compute_groups", "projects", "specs"):
            self.assertIsInstance(ws[field], dict, f"{field} 应是 dict 不是 list")
        for field in ("id", "name", "updated_at"):
            self.assertIn(field, ws)

    def test_no_specs_is_the_documented_steady_state(self):
        """``specs={}`` 是默认稳态（真实缓存 15/16），不是异常态。"""
        ws = cache_states.no_specs()[cache_states.WS_ID]
        self.assertEqual(ws["specs"], {})
        self.assertTrue(ws["compute_groups"], "计算组仍应存在，缺的只是 specs")

    def test_partial_specs_filters_down_to_empty(self):
        """非空 specs 但按计算组筛完是空 —— 比"没有 specs"更阴险。"""
        ws = cache_states.partial_specs_other_lcg()[cache_states.WS_ID]
        self.assertTrue(ws["specs"], "specs 本身非空")
        matched = [
            s
            for s in ws["specs"].values()
            if cache_states.LCG_ID in s.get("logic_compute_group_ids", [])
        ]
        self.assertEqual(matched, [], "按目标计算组筛完应为空")

    def test_spec_without_lcg_ids_has_no_membership_field(self):
        ws = cache_states.spec_without_lcg_ids()[cache_states.WS_ID]
        spec = next(iter(ws["specs"].values()))
        self.assertNotIn("logic_compute_group_ids", spec)
        self.assertNotIn("logic_compute_group_id", spec)

    def test_load_all_resources_filters_unavailable(self):
        """被平台禁用的工作空间默认不该出现在可见集合里。"""
        with sandbox_home(resources=cache_states.unavailable_flag()):
            self.assertEqual(config.load_all_resources(), {})
            everything = config.load_all_resources(include_unavailable=True)
            self.assertEqual(len(everything), 1)

    def test_load_all_resources_survives_corrupt_json(self):
        """非法 JSON 不该把命令打崩，应降级成空缓存。"""
        with sandbox_home(resources=cache_states.corrupt_json()):
            self.assertEqual(config.load_all_resources(), {})

    def test_load_all_resources_survives_non_dict_workspace(self):
        """工作空间值不是 dict 时，过滤逻辑不能抛 AttributeError。"""
        with sandbox_home(resources=cache_states.workspace_not_a_dict()):
            self.assertEqual(len(config.load_all_resources()), 1)

    def test_load_all_resources_on_missing_file(self):
        with sandbox_home(resources=cache_states.missing()):
            self.assertEqual(config.load_all_resources(), {})


if __name__ == "__main__":
    unittest.main()
