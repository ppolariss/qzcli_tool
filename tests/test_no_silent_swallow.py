"""异常不许被静默吞掉。

## 这个文件在防什么

`except Exception: pass` 写起来只有两行，代价是**故障现场彻底消失**。
这个仓库为它付过两笔账：

- `exec` 在开发机上卡满 120 秒然后报「命令执行超时」。真实原因（轮询每一轮
  拿到什么错）被 `pass` 掉了，报出来的话跟实际发生的事没有关系。
- `avail` 的「HPC 节点 CPU/内存利用率」整段消失过，因为整块 try 里任何一个
  异常都会让它跳过那个工作空间 —— 退出码 0，看着像"今天就是没有 HPC 节点"。

两种症状的共同点：**不报错、不红字、退出码 0**。人只会以为平台今天没数据。

## 三层防线

1. `test_no_broad_silent_swallow_in_repo` —— 全仓 AST 扫描，结构性禁止这个写法
2. `DiagTests` —— 被吞的异常确实留了痕，且 `QZCLI_DEBUG=1` 时能看见
3. `ExecTimeoutReasonTests` —— 超时报错必须带上真实原因，这是最初那笔账
"""

import ast
import io
import os
import pathlib
import sys
import unittest
from contextlib import redirect_stderr
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from qzcli import diag  # noqa: E402

REPO = pathlib.Path(__file__).resolve().parent.parent


class RepoWideSwallowScanTests(unittest.TestCase):
    """结构性地禁止 `except Exception: pass`。"""

    def _offenders(self):
        bad = []
        for path in sorted(REPO.rglob("*.py")):
            if any(p in path.parts for p in (".git", "build", ".venv", "dist")):
                continue
            try:
                tree = ast.parse(path.read_text(encoding="utf-8"))
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.ExceptHandler):
                    continue
                broad = node.type is None or ast.unparse(node.type) in (
                    "Exception",
                    "BaseException",
                )
                # 只判「纯 pass」。捕获后写日志 / 给默认值 / 转成友好报错都合法 ——
                # 罪状是**不留任何痕迹**，不是"捕获范围宽"本身。
                silent = all(isinstance(s, ast.Pass) for s in node.body)
                if broad and silent:
                    bad.append(f"{path.relative_to(REPO)}:{node.lineno}")
        return bad

    def test_no_broad_silent_swallow_in_repo(self):
        offenders = self._offenders()
        self.assertEqual(
            offenders,
            [],
            "发现静默吞异常的写法。捕获可以，但至少要 diag.swallowed(现场, exc)，"
            "否则故障现场会彻底消失：\n  " + "\n  ".join(offenders),
        )

    def test_scanner_actually_catches_the_shape(self):
        """扫描器自身的自检 —— 别写出一个永远返回空列表的"绿灯"测试。

        用**真实犯案形状**过一遍 AST 判定，而不是相信扫描器写对了。
        """
        src = "try:\n    f()\nexcept Exception:\n    pass\n"
        tree = ast.parse(src)
        handler = next(
            n for n in ast.walk(tree) if isinstance(n, ast.ExceptHandler)
        )
        broad = ast.unparse(handler.type) in ("Exception", "BaseException")
        silent = all(isinstance(s, ast.Pass) for s in handler.body)
        self.assertTrue(broad and silent, "扫描器判不出最基本的犯案形状")

        # 反面：有处理逻辑的不该被判为犯案
        ok = ast.parse("try:\n    f()\nexcept Exception as e:\n    log(e)\n")
        h2 = next(n for n in ast.walk(ok) if isinstance(n, ast.ExceptHandler))
        self.assertFalse(
            all(isinstance(s, ast.Pass) for s in h2.body),
            "记了日志的捕获被误判成静默吞",
        )


class DiagTests(unittest.TestCase):
    def setUp(self):
        diag.clear()

    def test_swallowed_is_silent_without_debug_env(self):
        with patch.dict(os.environ, {"QZCLI_DEBUG": ""}, clear=False):
            buf = io.StringIO()
            with redirect_stderr(buf):
                diag.swallowed("测试/现场", ValueError("boom"))
            self.assertEqual(buf.getvalue(), "", "默认不该往 stderr 喷东西")
        # 静默 ≠ 丢失
        self.assertEqual(diag.last_reason("测试/"), "ValueError: boom")

    def test_debug_env_makes_it_visible(self):
        with patch.dict(os.environ, {"QZCLI_DEBUG": "1"}, clear=False):
            buf = io.StringIO()
            with redirect_stderr(buf):
                diag.swallowed("测试/现场", ValueError("boom"))
            self.assertIn("测试/现场", buf.getvalue())
            self.assertIn("ValueError: boom", buf.getvalue())

    def test_debug_env_off_values(self):
        for off in ("", "0", "false", "no", "  "):
            with patch.dict(os.environ, {"QZCLI_DEBUG": off}, clear=False):
                self.assertFalse(diag.debug_enabled(), f"{off!r} 应算作关")
        for on in ("1", "true", "yes", "whatever"):
            with patch.dict(os.environ, {"QZCLI_DEBUG": on}, clear=False):
                self.assertTrue(diag.debug_enabled(), f"{on!r} 应算作开")

    def test_last_reason_matches_by_prefix_and_returns_newest(self):
        diag.swallowed("exec/轮询", OSError("第一次"))
        diag.swallowed("avail/取数", OSError("别的现场"))
        diag.swallowed("exec/轮询", OSError("最后一次"))
        self.assertIn("最后一次", diag.last_reason("exec/"))
        self.assertIsNone(diag.last_reason("不存在的现场/"))

    def test_ring_is_bounded(self):
        for i in range(500):
            diag.swallowed("压/测", ValueError(str(i)))
        self.assertLessEqual(len(diag.recent(1000)), 64)
        self.assertIn("499", diag.last_reason("压/"))


class ExecTimeoutReasonTests(unittest.TestCase):
    """超时报错必须说出真实原因 —— 最初那笔账。"""

    def setUp(self):
        diag.clear()

    def _run_timeout(self):
        from qzcli import cli

        msgs = []

        class _D:
            def __getattr__(self, _name):
                return lambda *a, **k: msgs.append(" ".join(str(x) for x in a))

        with patch.object(cli, "_exec_launch", return_value="job-x"), patch.object(
            cli, "_exec_poll", return_value=(124, "", False)
        ):
            cli._exec_via_jupyter(
                {"notebook_id": "nb-1", "base_url": "http://x", "token": "t"},
                "echo hi",
                _D(),
            )
        return "\n".join(msgs)

    def test_timeout_message_carries_the_swallowed_reason(self):
        diag.swallowed("exec/轮询", OSError("403 Forbidden"))
        text = self._run_timeout()
        self.assertIn("超时", text)
        self.assertIn(
            "403 Forbidden",
            text,
            "超时提示没带上轮询期间的真实失败原因 —— 这正是当初查不出 exec "
            "为什么卡满 120 秒的原因",
        )

    def test_timeout_message_stays_clean_when_nothing_was_swallowed(self):
        """没吞过东西就别硬加一行空提示。"""
        text = self._run_timeout()
        self.assertIn("超时", text)
        self.assertNotIn("轮询期间最后一次失败", text)


if __name__ == "__main__":
    unittest.main()
