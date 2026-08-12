"""``qzcli worker exec`` 的输出解析契约。

## 为什么要有这些测试

worker 容器的通道是**交互式 PTY**（WebSocket），不是「发命令返回 stdout」的
REST 接口 —— 平台没有那种接口。所以「拿到干净输出」这件事完全靠本模块自己在
流里切：起 shell → 发 ``<cmd>; echo <哨兵>_$?`` → 读到哨兵 → 清洗。

这段清洗逻辑很容易被后来的改动弄坏，而弄坏的表现是**输出里混进 banner／提示符／
ANSI 转义**，看着"还有输出"、不会报错 —— 正是本仓最忌讳的静默失败。所以钉住。

真机连通性不在这里测（要活着的训练任务），由 ``qzcli worker diag`` 人工验。
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qzcli.worker_exec import (  # noqa: E402
    _parse_output,
    default_instance_name,
    strip_ansi,
)


class StripAnsiTests(unittest.TestCase):
    def test_removes_csi_sequences(self):
        raw = "\x1b[32m绿色\x1b[0m普通"
        self.assertEqual(strip_ansi(raw), "绿色普通")

    def test_removes_osc_sequences(self):
        raw = "\x1b]0;window title\x07正文"
        self.assertEqual(strip_ansi(raw), "正文")

    def test_handles_bracketed_paste_markers(self):
        # PTY 会插 ``\x1b[?2004h`` 这类带 ? 的 CSI，普通 [0-9;]* 的正则会漏掉
        self.assertEqual(strip_ansi("\x1b[?2004h内容\x1b[?2004l"), "内容")


class ParseOutputTests(unittest.TestCase):
    """从 PTY 流里切出「命令自身输出」和 exit code。"""

    def _stream(self, command, mark, body, code, prompt="[root:user-123]$ "):
        """拼一段和真机同构的 PTY 流：回显 + 输出 + 哨兵 + 提示符。"""
        return (
            f"{command}; echo {mark}_$?\r\n"
            f"{body}\r\n"
            f"{mark}_{code}\r\n"
            f"{prompt}"
        )

    def test_extracts_exit_code_and_body(self):
        mark = "QZW_1"
        raw = self._stream("hostname", mark, "gpu-node-7", 0)
        code, out = _parse_output(raw, "hostname", mark)
        self.assertEqual(code, 0)
        self.assertEqual(out, "gpu-node-7")

    def test_nonzero_exit_code(self):
        mark = "QZW_2"
        raw = self._stream("false", mark, "", 1)
        code, _ = _parse_output(raw, "false", mark)
        self.assertEqual(code, 1)

    def test_trailing_prompt_is_removed(self):
        """提示符是 PTY 的产物，混进输出就是每次都拖一行噪声。"""
        mark = "QZW_3"
        raw = self._stream("echo hi", mark, "hi", 0)
        _, out = _parse_output(raw, "echo hi", mark)
        self.assertNotIn("$", out)
        self.assertEqual(out, "hi")

    def test_command_echo_is_not_part_of_output(self):
        """命令回显要切掉，否则输出里会出现用户刚敲的那条命令。"""
        mark = "QZW_4"
        raw = self._stream("nvidia-smi -L", mark, "GPU 0: NVIDIA H200", 0)
        _, out = _parse_output(raw, "nvidia-smi -L", mark)
        self.assertFalse(out.startswith("nvidia-smi"))
        self.assertEqual(out, "GPU 0: NVIDIA H200")

    def test_multiline_body_preserved(self):
        mark = "QZW_5"
        body = "0, 100 %\n1, 98 %\n2, 97 %"
        raw = self._stream(
            "nvidia-smi --query-gpu=index,utilization.gpu", mark, body, 0
        )
        _, out = _parse_output(
            raw, "nvidia-smi --query-gpu=index,utilization.gpu", mark
        )
        self.assertEqual(out.splitlines(), ["0, 100 %", "1, 98 %", "2, 97 %"])

    def test_missing_marker_gives_minus_one(self):
        """哨兵没出现（超时/连接断）时不能假装成功。"""
        code, _ = _parse_output("一些没头没尾的输出", "hostname", "QZW_6")
        self.assertEqual(code, -1)

    def test_ansi_in_stream_is_cleaned(self):
        mark = "QZW_7"
        raw = f"hostname; echo {mark}_$?\r\n\x1b[32mgpu-1\x1b[0m\r\n{mark}_0\r\n"
        code, out = _parse_output(raw, "hostname", mark)
        self.assertEqual(code, 0)
        self.assertEqual(out, "gpu-1")


class InstanceNameTests(unittest.TestCase):
    def test_default_is_worker_zero(self):
        self.assertEqual(
            default_instance_name("job-abc"),
            "job-abc-worker-0",
        )

    def test_index_is_respected(self):
        """大规模任务要能指定第 N 个 worker（排查掉队节点的刚需）。"""
        self.assertEqual(
            default_instance_name("job-abc", 104),
            "job-abc-worker-104",
        )


if __name__ == "__main__":
    unittest.main()
