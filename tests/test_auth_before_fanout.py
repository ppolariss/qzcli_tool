"""并发扇出之前必须先确认鉴权。

## 这条规则是拿一个被锁的账号换来的

2026-08-12，账号被 CAS 锁死：

    用户名或密码错误：您的账号被锁定，请联系管理员。

根因不是偶发，是**架构反了**：本仓每个 API 方法都挂 ``@with_auth_retry``，
撞 401 就自己重登。单线程没问题；从 N 个并发 worker 调用时，cookie 失效那一刻
**N 个 worker 同时撞 401、同时去登录**。CAS 按失败次数延长锁定 —— 越重试锁越死。

为此陆续加过四层保护：

===========  ==========================================  ====================
时间         补丁                                        治的是什么
===========  ==========================================  ====================
v0.4.1       进程内锁 + 跨进程文件锁                     抢着登录时怎么办
``49fe82b``  补一条绕过路径 + 60s 失败冷却               抢着登录时怎么办
``ea1a210``  按「失败的那个 cookie」去重                 抢着登录时怎么办
``a0cb5c2``  凭据类失败永不自动重试                      抢着登录时怎么办
===========  ==========================================  ====================

**四层全在治「抢着登录时怎么办」，没有一层在治「为什么要让 worker 去登录」。**
每次发现新的绕过路径就再补一层 —— ``49fe82b`` 的 commit message 自己写着
「同构代码只修一半，这已经是本轮第三次」。

对照 inspire 插件（``common-shared-logic`` 技能）：它**完全没有自动重登**，
401 直接让用户去登录；并且把「拉 permissions / user detail 建立鉴权上下文」
列为业务动作之前的固定步骤。**鉴权是前置的一次性步骤，不是每个调用的自愈行为。**

## 所以这条测试钉的是结构，不是行为

靠人记得「新加并发时要先鉴权」是靠不住的 —— 前四次都没记住。这里用 AST 穷举：
**任何含 ``ThreadPoolExecutor`` 的函数，函数体里必须出现前置鉴权调用。**

判据故意宽松（只要求"出现"，不校验位置），因为严格的顺序分析会引入误报，
而误报会让人开始忽略这道闸门 —— 那比不设闸门更糟。
"""

import ast
import pathlib
import unittest

REPO = pathlib.Path(__file__).resolve().parent.parent

#: 认可的前置鉴权调用。两种写法：
#: - ``QzAPI.ensure_authenticated``（api.py 内部、或持有 api 对象的调用方）
#: - ``cli._ensure_auth_before_fanout``（容错包装，测试替身没有该方法时原样返回）
_PREFLIGHT_NAMES = ("ensure_authenticated", "_ensure_auth_before_fanout")

#: 扫这些文件。工具脚本（tools/）不在内 —— 它们不是随包发布的用户路径。
_SCANNED = ("qzcli/api.py", "qzcli/cli.py", "qzcli/dashboard_app.py")


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
        # 定义本身不算（ensure_authenticated 自己不扇出）
        has = any(name in body for name in _PREFLIGHT_NAMES)
        yield node.name, node.lineno, has


class AuthBeforeFanoutTests(unittest.TestCase):
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
            "  cookie 失效那一刻，每个 worker 都会各自撞 401 去登录，\n"
            "  CAS 按失败次数延长锁定 —— 2026-08-12 账号就是这么锁死的。\n"
            "  加一行：cookie = _ensure_auth_before_fanout(api, cookie)\n"
            "  （或 self.ensure_authenticated()），放在 ThreadPoolExecutor 之前。\n"
            "  漏的是：\n    " + "\n    ".join(missing),
        )

    def test_scanner_actually_finds_the_fanouts(self):
        """扫描器自检 —— 别写出一个永远返回空列表的绿灯测试。

        本仓实测有 7 个并发扇出点。数量会变，所以只断言「找得到若干个」，
        并且断言其中包含几个**点名的**已知扇出函数。
        """
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


if __name__ == "__main__":
    unittest.main()
