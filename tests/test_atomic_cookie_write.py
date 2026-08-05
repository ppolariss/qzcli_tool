"""cookie 落盘必须是原子的。

## 病理

``save_cookie`` 原本是 ``open(path, "w")`` 然后 ``json.dump``。截断和写完之间有一个
窗口，文件是空的或半截的。并发读的线程/进程这时 ``json.load`` 会失败，而所有读取点
都把失败当成「没有这个文件」处理，于是 ``get_cookie()`` 返回 ``None``。

**实测后果**：8 个并发登录里偶发出现 **2 次真实 CAS 登录** —— 某个线程在这个窗口里
读到 ``None``，``_relogin`` 的去重判据 ``if current and current != baseline``
判假，于是又打了一次 CAS。而反复登录正是把账号推进验证码锁定的那个动作。

在别处则可能表现为莫名其妙的「未设置 cookie，请先运行 qzcli login」。

修法是写临时文件 + ``os.replace``（同文件系统上原子），读者要么看到旧内容、
要么看到新内容，不会看到中间态。

## 临时文件名必须每次唯一

第一版用 ``path + ".tmp." + str(os.getpid())`` —— 同一进程的多个**线程**会撞同一个
名字，然后互相把对方还没 replace 的临时文件删掉，直接 ``FileNotFoundError``。
用 ``tempfile.mkstemp`` 由内核保证唯一。
"""

import json
import os
import sys
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from support.sandbox import sandbox_home  # noqa: E402

from qzcli import config  # noqa: E402


class AtomicCookieWriteTests(unittest.TestCase):
    def test_concurrent_readers_never_see_a_broken_file(self):
        """一边狂写一边狂读，读到的必须**要么是旧值要么是新值**，不能是 None。

        修复前这里会读到 None（撞上截断窗口）。
        """
        with sandbox_home(cookie='{"cookie": "v0", "workspace_id": "ws-1"}'):
            stop = threading.Event()
            bad = []

            def writer():
                i = 0
                while not stop.is_set():
                    i += 1
                    config.save_cookie(f"inspire-session=v{i}", "ws-1")

            def reader():
                while not stop.is_set():
                    got = config.get_cookie()
                    if not got or not got.get("cookie"):
                        bad.append(got)

            with ThreadPoolExecutor(max_workers=6) as pool:
                futs = [pool.submit(writer) for _ in range(2)]
                futs += [pool.submit(reader) for _ in range(4)]
                threading.Event().wait(1.5)
                stop.set()
                for f in futs:
                    f.result()

            self.assertEqual(
                bad, [], f"并发读撞到了 {len(bad)} 次空/半截 cookie —— 写入不是原子的"
            )

    def test_concurrent_writers_do_not_delete_each_others_temp_files(self):
        """多线程同时写不能互相删临时文件。

        临时文件名只带 PID 时，同进程的线程会撞名，然后 FileNotFoundError。
        """
        with sandbox_home(cookie='{"cookie": "v0", "workspace_id": "ws-1"}'):
            errors = []

            def write(i):
                try:
                    config.save_cookie(f"inspire-session=w{i}", "ws-1")
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{type(exc).__name__}: {exc}")

            with ThreadPoolExecutor(max_workers=16) as pool:
                list(pool.map(write, range(64)))

            self.assertEqual(errors, [], f"并发写报错：{errors[:3]}")
            self.assertTrue(config.get_cookie(), "写完之后 cookie 应该可读")

    def test_no_temp_files_left_behind(self):
        """成功路径不该留下临时文件残渣。"""
        with sandbox_home() as sandbox_dir:
            for i in range(20):
                config.save_cookie(f"inspire-session=x{i}", "ws-1")
            leftovers = [p.name for p in sandbox_dir.iterdir() if ".tmp." in p.name]
            self.assertEqual(leftovers, [], f"残留临时文件: {leftovers}")

    def test_content_round_trips(self):
        """原子写不能把内容写坏 —— 对照组。"""
        with sandbox_home():
            config.save_cookie("inspire-session=abc", "ws-9")
            got = config.get_cookie() or {}
            self.assertEqual(got.get("cookie"), "inspire-session=abc")
            self.assertEqual(got.get("workspace_id"), "ws-9")
            self.assertIn("saved_at", got)


if __name__ == "__main__":
    unittest.main()
