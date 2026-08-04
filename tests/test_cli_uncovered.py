"""此前零覆盖的命令。

这批命令一次都没被测过（`grep cmd_init/cmd_watch/cmd_track/... tests/` 全空），
而它们全都读写本地状态 —— `jobs.json` / `.cookie`。也就是说「误删用户任务记录」
「把 cookie 写坏」这类事故一直没有任何防线。

全部在 `sandbox_home` 里跑：这些用例会真的写盘，不隔离就会动用户真实的 `~/.qzcli`。

## 断言口径

沿用缓存矩阵那套：**不是断言返回码，而是断言「做了正确的事 / 给出正确且可执行的
错误」**。返回码对但内容是假的，正是之前那批 bug 的形状。

对交互式命令（`remove` / `clear` / `cookie`）重点钉**否定路径** ——
用户回答 "n" 时必须真的什么都没删。这类 bug 一旦有，损失不可逆。
"""

import argparse
import io
import json
import os
import sys
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from support.sandbox import real_home, sandbox_home  # noqa: E402

from qzcli import cli  # noqa: E402
from qzcli import store as store_mod  # noqa: E402
from qzcli.api import QzAPIError  # noqa: E402
from qzcli.store import JobRecord, JobStore  # noqa: E402


class _Display:
    """收集输出的假 display；用 __getattr__ 兜住 print_* 家族的增减。"""

    def __init__(self):
        self.lines = []

    def print(self, msg="", *a, **kw):
        self.lines.append(str(msg))

    def __getattr__(self, name):
        if name.startswith("print"):
            return self.print
        raise AttributeError(name)

    @property
    def text(self):
        return "\n".join(self.lines)


class _CmdTestCase(unittest.TestCase):
    """公共脚手架：沙箱 HOME + 独立 JobStore + 假 display。"""

    def setUp(self):
        self._sandbox = sandbox_home()
        self.home = self._sandbox.__enter__()
        self.addCleanup(self._sandbox.__exit__, None, None, None)

        # get_store() 是全局单例，且 JOBS_FILE 在模块加载时求值 —— 必须两个都换掉，
        # 否则测试会写到真实的 ~/.qzcli/jobs.json
        self.store = JobStore(self.home / "jobs.json")
        p = patch.object(cli, "get_store", return_value=self.store)
        p.start()
        self.addCleanup(p.stop)

        self.display = _Display()
        p2 = patch.object(cli, "get_display", return_value=self.display)
        p2.start()
        self.addCleanup(p2.stop)

    def add_job(self, job_id, status="job_running", **kw):
        self.store.add(JobRecord(job_id=job_id, status=status, **kw))
        return job_id


class RemoveTests(_CmdTestCase):
    def test_declining_confirmation_removes_nothing(self):
        """回答 n 必须真的什么都不删 —— 这类 bug 的损失不可逆。"""
        self.add_job("job-keep")
        args = argparse.Namespace(job_id="job-keep", yes=False)
        with patch("builtins.input", return_value="n"):
            rc = cli.cmd_remove(args)
        self.assertEqual(rc, 0)
        self.assertEqual(self.store.count(), 1, "回答 n 却把记录删了")
        self.assertIn("已取消", self.display.text)

    def test_empty_answer_is_treated_as_no(self):
        """直接回车（默认 N）也不能删。"""
        self.add_job("job-keep")
        args = argparse.Namespace(job_id="job-keep", yes=False)
        with patch("builtins.input", return_value=""):
            cli.cmd_remove(args)
        self.assertEqual(self.store.count(), 1)

    def test_yes_flag_skips_prompt_and_removes(self):
        self.add_job("job-gone")
        args = argparse.Namespace(job_id="job-gone", yes=True)
        with patch("builtins.input", side_effect=AssertionError("不该询问")):
            rc = cli.cmd_remove(args)
        self.assertEqual(rc, 0)
        self.assertEqual(self.store.count(), 0)

    def test_missing_job_reports_actionable_error(self):
        args = argparse.Namespace(job_id="job-nope", yes=True)
        rc = cli.cmd_remove(args)
        self.assertEqual(rc, 1)
        self.assertIn("job-nope", self.display.text, "错误里要带上是哪个任务")


class ClearTests(_CmdTestCase):
    def test_declining_confirmation_keeps_everything(self):
        for i in range(3):
            self.add_job(f"job-{i}")
        args = argparse.Namespace(yes=False)
        with patch("builtins.input", return_value="n"):
            rc = cli.cmd_clear(args)
        self.assertEqual(rc, 0)
        self.assertEqual(self.store.count(), 3, "回答 n 却清空了")

    def test_yes_clears_all(self):
        for i in range(3):
            self.add_job(f"job-{i}")
        rc = cli.cmd_clear(argparse.Namespace(yes=True))
        self.assertEqual(rc, 0)
        self.assertEqual(self.store.count(), 0)

    def test_empty_store_does_not_prompt(self):
        """本来就是空的就别多问一句。"""
        with patch("builtins.input", side_effect=AssertionError("不该询问")):
            rc = cli.cmd_clear(argparse.Namespace(yes=False))
        self.assertEqual(rc, 0)

    def test_confirmation_message_states_the_count(self):
        """提示里要说清будет清掉几个 —— 用户据此判断是不是搞错了库。"""
        for i in range(7):
            self.add_job(f"job-{i}")
        captured = {}

        def _fake_input(prompt=""):
            captured["prompt"] = prompt
            return "n"

        with patch("builtins.input", _fake_input):
            cli.cmd_clear(argparse.Namespace(yes=False))
        self.assertIn("7", captured.get("prompt", ""))


class TrackTests(_CmdTestCase):
    def _args(self, **kw):
        base = dict(job_id="job-1", name="", source="", workspace="", quiet=True)
        base.update(kw)
        return argparse.Namespace(**base)

    def test_falls_back_to_minimal_record_when_api_fails(self):
        """API 拿不到详情也要把 job 记下来 —— 否则脚本提交完就丢了。"""
        api = MagicMock()
        api.get_job_detail.side_effect = QzAPIError("boom")
        with patch.object(cli, "get_api", return_value=api):
            rc = cli.cmd_track(self._args(job_id="job-x", name="我的任务"))
        self.assertEqual(rc, 0)
        jobs = self.store.list()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].job_id, "job-x")
        self.assertEqual(jobs[0].name, "我的任务")

    def test_explicit_metadata_overrides_api_values(self):
        """显式传的 name/source/workspace 要盖过 API 返回的。"""
        api = MagicMock()
        api.get_job_detail.return_value = {
            "job_id": "job-y",
            "name": "平台上的名字",
            "status": "job_running",
        }
        with patch.object(cli, "get_api", return_value=api):
            cli.cmd_track(
                self._args(job_id="job-y", name="我起的名字", workspace="ws-9")
            )
        job = self.store.list()[0]
        self.assertEqual(job.name, "我起的名字")
        self.assertEqual(job.workspace_id, "ws-9")

    def test_quiet_suppresses_output(self):
        api = MagicMock()
        api.get_job_detail.side_effect = QzAPIError("boom")
        with patch.object(cli, "get_api", return_value=api):
            cli.cmd_track(self._args(quiet=True))
        self.assertNotIn("已追踪", self.display.text)


class ImportTests(_CmdTestCase):
    def _write(self, content):
        path = self.home / "ids.txt"
        path.write_text(content, encoding="utf-8")
        return path

    def test_missing_file_is_an_actionable_error(self):
        args = argparse.Namespace(
            file=str(self.home / "nope.txt"), source="", refresh=False
        )
        rc = cli.cmd_import(args)
        self.assertEqual(rc, 1)
        self.assertIn("nope.txt", self.display.text)

    def test_imports_ids_and_skips_comments(self):
        path = self._write("# 注释\njob-a\n\njob-b\n不是任务ID\n")
        args = argparse.Namespace(file=str(path), source="", refresh=False)
        with patch.object(cli, "get_api", return_value=MagicMock()):
            rc = cli.cmd_import(args)
        self.assertEqual(rc, 0)
        ids = {j.job_id for j in self.store.list()}
        self.assertEqual(ids, {"job-a", "job-b"}, "只收 job- 前缀，注释和杂行要跳过")

    def test_no_refresh_makes_no_network_call(self):
        """不带 --refresh 时必须零网络 —— 导入是纯本地操作。"""
        path = self._write("job-a\n")
        api = MagicMock()
        args = argparse.Namespace(file=str(path), source="", refresh=False)
        with patch.object(cli, "get_api", return_value=api):
            cli.cmd_import(args)
        api.get_jobs_detail.assert_not_called()

    def test_refresh_failure_does_not_lose_the_import(self):
        """状态刷新失败也不能把已导入的记录弄丢。"""
        path = self._write("job-a\n")
        api = MagicMock()
        api.get_jobs_detail.side_effect = QzAPIError("网络炸了")
        args = argparse.Namespace(file=str(path), source="", refresh=True)
        with patch.object(cli, "get_api", return_value=api):
            rc = cli.cmd_import(args)
        self.assertEqual(rc, 0)
        self.assertEqual(self.store.count(), 1, "刷新失败不该回滚导入")


class CookieTests(_CmdTestCase):
    def _args(self, **kw):
        base = dict(
            cookie=None, file=None, workspace="", show=False, clear=False, no_test=True
        )
        base.update(kw)
        return argparse.Namespace(**base)

    def test_show_on_empty_does_not_crash(self):
        rc = cli.cmd_cookie(self._args(show=True))
        self.assertEqual(rc, 0)
        self.assertIn("未设置", self.display.text)

    def test_save_and_show_roundtrip(self):
        cli.cmd_cookie(self._args(cookie="inspire-session=abc", workspace="ws-1"))
        self.display.lines.clear()
        cli.cmd_cookie(self._args(show=True))
        self.assertIn("ws-1", self.display.text)

    def test_clear_removes_cookie(self):
        cli.cmd_cookie(self._args(cookie="inspire-session=abc"))
        rc = cli.cmd_cookie(self._args(clear=True))
        self.assertEqual(rc, 0)
        from qzcli import config

        self.assertIsNone(config.get_cookie())

    def test_empty_cookie_is_rejected(self):
        """空 cookie 不能被存进去 —— 存了之后每条命令都会 401。"""
        with patch("builtins.input", return_value="   "):
            rc = cli.cmd_cookie(self._args())
        self.assertEqual(rc, 1)
        from qzcli import config

        self.assertIsNone(config.get_cookie())

    def test_reads_last_valid_line_from_file(self):
        path = self.home / "c.txt"
        path.write_text("# 说明\ncookie\ninspire-session=from-file\n", encoding="utf-8")
        rc = cli.cmd_cookie(self._args(file=str(path)))
        self.assertEqual(rc, 0)
        from qzcli import config

        self.assertEqual(
            (config.get_cookie() or {}).get("cookie"), "inspire-session=from-file"
        )

    def test_invalid_cookie_is_not_saved(self):
        """验证失败时**绝不能**把坏 cookie 落盘 —— 否则会顶掉正在用的好 cookie。"""
        from qzcli import config

        config.save_cookie("inspire-session=good", "ws-1")
        api = MagicMock()
        api.list_jobs_with_cookie.side_effect = QzAPIError("Cookie 无效", 401)
        with patch.object(cli, "get_api", return_value=api):
            rc = cli.cmd_cookie(
                self._args(
                    cookie="inspire-session=bad", workspace="ws-1", no_test=False
                )
            )
        self.assertEqual(rc, 1)
        self.assertEqual(
            (config.get_cookie() or {}).get("cookie"),
            "inspire-session=good",
            "验证失败的 cookie 把原来能用的顶掉了",
        )


class WatchTests(_CmdTestCase):
    """``cmd_watch``。

    注意它会往 stdout 打裸 ANSI 清屏序列（``\033[2J\033[H``），不接住的话跑测试
    会把开发者的终端清掉。顺带说明一个真实风险：走 MCP stdio 的路径上出现这种裸
    ``print`` 会直接破坏协议 —— 目前 exec 那条靠 ``_CollectingDisplay`` 接住了。
    """

    def _run(self, args):
        with redirect_stdout(io.StringIO()):
            return cli.cmd_watch(args)

    def _args(self, **kw):
        base = dict(interval=0, limit=30, keep_alive=False)
        base.update(kw)
        return argparse.Namespace(**base)

    def test_exits_when_all_jobs_are_terminal(self):
        """全终态就该收工，不能空转。"""
        self.add_job("job-1", status="job_succeeded")
        self.add_job("job-2", status="job_failed")
        api = MagicMock()
        api.get_jobs_detail.return_value = {}
        with patch.object(cli, "get_api", return_value=api), patch.object(
            cli.time, "sleep", side_effect=AssertionError("全终态不该再 sleep")
        ):
            rc = self._run(self._args())
        self.assertEqual(rc, 0)
        self.assertIn("所有任务已完成", self.display.text)

    def test_keep_alive_does_not_exit_on_all_terminal(self):
        """-k 时即使全终态也要继续守着，只能靠中断退出。"""
        self.add_job("job-1", status="job_succeeded")
        api = MagicMock()
        api.get_jobs_detail.return_value = {}
        with patch.object(cli, "get_api", return_value=api), patch.object(
            cli.time, "sleep", side_effect=KeyboardInterrupt
        ):
            rc = self._run(self._args(keep_alive=True))
        self.assertEqual(rc, 0)
        self.assertIn("监控已停止", self.display.text)

    def test_ctrl_c_exits_cleanly(self):
        """有活跃任务时 Ctrl-C 要干净退出，不能把 KeyboardInterrupt 抛给用户。"""
        self.add_job("job-1", status="job_running")
        api = MagicMock()
        api.get_jobs_detail.return_value = {}
        with patch.object(cli, "get_api", return_value=api), patch.object(
            cli.time, "sleep", side_effect=KeyboardInterrupt
        ):
            rc = self._run(self._args())
        self.assertEqual(rc, 0)


class StoreIsolationTests(_CmdTestCase):
    def test_tests_do_not_touch_the_real_jobs_file(self):
        """自检：这一批用例全程只写沙箱里的 jobs.json。

        不解析结构、直接查字符串 —— 自检本身要对被检对象的格式变化免疫，
        否则哪天 jobs.json 换了形状，自检会以「格式看不懂」的姿态假绿。
        """
        marker = "job-sandbox-selfcheck-marker"
        self.add_job(marker)
        sandbox_file = self.home / "jobs.json"
        self.assertTrue(sandbox_file.exists())
        self.assertIn(marker, sandbox_file.read_text(encoding="utf-8"))

        # 必须用 real_home()：沙箱里 expanduser("~") 返回的是沙箱自己
        real = real_home() / ".qzcli" / "jobs.json"
        if real.exists():
            self.assertNotIn(
                marker,
                real.read_text(encoding="utf-8"),
                "测试数据写进了真实 jobs.json",
            )


if __name__ == "__main__":
    unittest.main()
