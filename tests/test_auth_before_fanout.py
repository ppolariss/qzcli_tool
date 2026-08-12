"""登录只在允许的线程发生；并发扇出之前先确认鉴权。

## 这套规则是拿一个被锁的账号换来的

2026-08-12，账号被 CAS 锁死：

    用户名或密码错误：您的账号被锁定，请联系管理员。

根因是**架构反了**：每个 API 方法都挂 ``@with_auth_retry``，撞 401 就自己重登。
单线程没问题；从 N 个并发 worker 调用时，cookie 失效那一刻 **N 个 worker 同时撞
401、同时去登录**。CAS 按失败次数延长锁定 —— 越重试锁越死。

历史上为此加过四层保护（进程内锁、跨进程文件锁、按失败 cookie 去重、失败冷却），
**全在治「抢着登录时怎么办」，没有一层在治「为什么让 worker 去登录」**。

## 现在的设计（两条互补的防线）

1. **装饰器判据**（``api._relogin_allowed``）：自动重登只在「主线程 or
   ``ensure_authenticated`` 窗口内」发生。worker 线程两者皆非 —— 撞 401 直接抛
   ``QzAuthExpiredError``，不去打 CAS。这是**行为**层的防线，本文件下半段直接验它。

2. **前置鉴权**（``ensure_authenticated`` / ``_ensure_auth_before_fanout``）：
   扇出前在主线程把 cookie 刷好，worker 正常情况下根本撞不到 401。这是**结构**层
   的防线，本文件上半段用 AST 扫描守它 —— 每个含 ``ThreadPoolExecutor`` 的函数
   必须有前置鉴权。

对照 inspire：它连自动重登都没有，401 直接让用户去 ``inspire_login``。qzcli 保留
主线程自愈是为了向后兼容（单线程命令 cookie 过期仍自动续，用户无感）。

## 为什么结构扫描只查前置鉴权、不再查 worker 禁令

早先版本要求每个扇出点显式包一层 ``no_worker_relogin`` 上下文。现在 worker 禁令
**内建进装饰器判据**了（worker 天然「非主线程 + 无窗口」），那个上下文已删除。
所以结构扫描回归到只查前置鉴权这一件。
"""

import ast
import pathlib
import threading
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent

#: 认可的前置鉴权调用。
_PREFLIGHT_NAMES = ("ensure_authenticated", "_ensure_auth_before_fanout")

#: 扫这些文件。工具脚本（tools/）不在内 —— 不是随包发布的用户路径。
_SCANNED = ("qzcli/api.py", "qzcli/cli.py", "qzcli/dashboard_app.py")

#: 机制自身的定义不是扇出点。它们 docstring 里会出现 ThreadPoolExecutor
#: （在说明该配合谁用），不能被当成"漏了预检的扇出"。
_MECHANISM_DEFS = {
    "ensure_authenticated",
    "_ensure_auth_before_fanout",
}


def _fanout_functions(path: pathlib.Path):
    """产出 ``(函数名, 行号, 是否有前置鉴权)``。"""
    src = path.read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = ast.get_source_segment(src, node) or ""
        if "ThreadPoolExecutor" not in body:
            continue
        if node.name in _MECHANISM_DEFS:
            continue
        has = any(name in body for name in _PREFLIGHT_NAMES)
        yield node.name, node.lineno, has


class AuthBeforeFanoutTests(unittest.TestCase):
    """结构防线：每个扇出点扇出前必须确认鉴权。"""

    def test_every_fanout_has_preflight_auth(self):
        missing = []
        checked = 0
        for rel in _SCANNED:
            for name, lineno, has in _fanout_functions(REPO / rel):
                checked += 1
                if not has:
                    missing.append(f"{rel}:{lineno} {name}")
        self.assertGreater(checked, 0, "一个扇出点都没扫到 —— 扫描器本身坏了")
        self.assertEqual(
            missing,
            [],
            "这些函数并发扇出，但扇出前没有确认鉴权。\n"
            "  少了它，worker 撞 401 会抛 QzAuthExpiredError（装饰器不许 worker 登录），\n"
            "  于是整条命令失败。扇出前加一行让主线程先把 cookie 刷好：\n"
            "    cookie = _ensure_auth_before_fanout(api, cookie)\n"
            "  （或 self.ensure_authenticated()），放在 ThreadPoolExecutor 之前。\n"
            "  漏的是：\n    " + "\n    ".join(missing),
        )

    def test_scanner_actually_finds_the_fanouts(self):
        """扫描器自检 —— 别写出一个永远返回空列表的绿灯测试。"""
        found = {
            name for rel in _SCANNED for name, _, _ in _fanout_functions(REPO / rel)
        }
        self.assertGreaterEqual(
            len(found), 5, f"只扫到 {len(found)} 个，扫描器可能失效"
        )
        for known in ("cmd_avail", "get_jobs_detail", "build_node_to_lcg_map"):
            self.assertIn(known, found, f"已知的扇出函数 {known} 没被扫到")

    def test_scanner_would_catch_a_bare_fanout(self):
        """把判定逻辑本身过一遍：没有前置鉴权的扇出必须被判为缺失。"""
        src = (
            "def f(api, cookie):\n"
            "    with ThreadPoolExecutor(max_workers=4) as ex:\n"
            "        ex.map(g, xs)\n"
        )
        tree = ast.parse(src)
        fn = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef))
        body = ast.get_source_segment(src, fn) or ""
        self.assertIn("ThreadPoolExecutor", body)
        self.assertFalse(
            any(n in body for n in _PREFLIGHT_NAMES),
            "判定逻辑认不出「裸扇出」，那这道闸门是摆设",
        )


class ReloginAllowedByThreadTests(unittest.TestCase):
    """行为防线：**谁被允许自动重登**。这是账号被锁的直接判据。

    比结构扫描强 —— 它验的是真实运行时行为，不是"代码里有没有那行字"。
    """

    def setUp(self):
        from qzcli import api

        self.api = api

    def test_main_thread_is_allowed(self):
        # 单线程命令都跑在主线程，必须允许自愈 —— 这是向后兼容的根基
        self.assertTrue(self.api._relogin_allowed())

    def test_worker_thread_is_not_allowed(self):
        """worker 线程默认不允许 —— 这一条就是防止「N 个 worker 抢着登录」。"""
        result = {}

        def work():
            result["allowed"] = self.api._relogin_allowed()

        t = threading.Thread(target=work)
        t.start()
        t.join()
        self.assertFalse(
            result["allowed"],
            "worker 线程被允许自动重登 —— 这正是把账号锁死的那条路",
        )

    def test_window_lets_a_worker_thread_self_heal(self):
        """非主线程若显式开窗（如看板/MCP 工作线程调 ensure_authenticated），
        那一次串行探针应能自愈。"""
        result = {}

        def work():
            with self.api._allow_relogin_here():
                result["in_window"] = self.api._relogin_allowed()
            result["after_window"] = self.api._relogin_allowed()

        t = threading.Thread(target=work)
        t.start()
        t.join()
        self.assertTrue(result["in_window"], "开窗后仍不允许 —— 探针无法自愈")
        self.assertFalse(result["after_window"], "窗口没有正确关闭 —— 泄漏到窗口之外")

    def test_worker_401_raises_auth_expired_not_relogin(self):
        """核心行为：装了 @with_auth_retry 的方法，在 worker 线程撞 401 时
        抛 QzAuthExpiredError，而**不**去调 _relogin。"""
        api = self.api

        class _Stub:
            _auto_relogin = True

            def __init__(self):
                self.relogin_called = False

            def _relogin(self, *a, **k):
                self.relogin_called = True
                return "new-cookie"

            @api.with_auth_retry
            def do(self, cookie=""):
                # 位置参数 = status_code(int)，与真实 401 构造一致，
                # 这样 exc.code == 401 才成立（code = status_code or api_code）。
                raise api.QzAPIError("Cookie 已过期或无效", 401)

        stub = _Stub()
        outcome = {}

        def work():
            try:
                stub.do(cookie="stale")
            except api.QzAuthExpiredError:
                outcome["raised"] = "auth_expired"
            except Exception as exc:  # noqa: BLE001
                outcome["raised"] = type(exc).__name__

        t = threading.Thread(target=work)
        t.start()
        t.join()
        self.assertEqual(
            outcome.get("raised"),
            "auth_expired",
            "worker 撞 401 没有抛 QzAuthExpiredError",
        )
        self.assertFalse(
            stub.relogin_called,
            "worker 撞 401 竟然调了 _relogin —— 禁令没生效",
        )

    def test_main_thread_401_does_relogin(self):
        """对照：同样的方法在**主线程**撞 401，应当调 _relogin 自愈（向后兼容）。"""
        api = self.api

        class _Stub:
            _auto_relogin = True

            def __init__(self):
                self.relogin_called = False
                self.calls = 0

            def _relogin(self, *a, **k):
                self.relogin_called = True
                return "new-cookie"

            @api.with_auth_retry
            def do(self, cookie=""):
                self.calls += 1
                if self.calls == 1:
                    raise api.QzAPIError("Cookie 已过期或无效", 401)
                return "ok"

        stub = _Stub()
        result = stub.do(cookie="stale")  # 主线程直接调
        self.assertTrue(stub.relogin_called, "主线程撞 401 没有自愈 —— 破坏向后兼容")
        self.assertEqual(result, "ok")


if __name__ == "__main__":
    unittest.main()
