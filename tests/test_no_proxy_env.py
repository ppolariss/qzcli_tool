"""``NO_PROXY`` 必须对 qzcli 生效。

## 这条是被一次真实故障逼出来的

2026-08-10，2224 上的看板挂了。根因是那个进程的环境里带着
``https_proxy=http://127.0.0.1:7891``（clash），而 ``qz.sii.edu.cn`` 走那个代理
是 SSL EOF；直连正常。

排查的人加了 ``no_proxy=.sii.edu.cn`` —— **没用**。因为 qzcli 自建 urllib3
pool manager，CAS 登录那条还显式设了 ``trust_env=False``，把 requests 自带的
``no_proxy`` 处理整个绕开了。最后只能把整个代理环境清空，副作用是那个进程
从此完全没法用代理访问外网。

也就是说：**只要环境里有代理变量，qzcli 眼里 ``no_proxy`` 就是不存在的。**
这不是配置问题，是 qzcli 的缺陷。

## 判据

对照组用 ``requests`` 自己的判断 —— 它是对的，qzcli 应该和它一致。
"""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402

from qzcli import config as cfg  # noqa: E402

PROXY = "http://127.0.0.1:7891"
TARGET = "https://qz.sii.edu.cn/api/v2/train"


class NoProxyEnvTests(unittest.TestCase):
    def setUp(self):
        # config.json 里配了代理会盖过环境变量，测试里统一清空
        self._p = patch.object(cfg, "load_config", return_value={})
        self._p.start()
        self.addCleanup(self._p.stop)

    def _env(self, **kw):
        base = {k: "" for k in ("ALL_PROXY", "HTTPS_PROXY", "NO_PROXY", "no_proxy")}
        base.update(kw)
        return patch.dict(os.environ, base, clear=False)

    def test_no_proxy_makes_qzcli_go_direct(self):
        """本条就是那次故障的最小复现。"""
        with self._env(ALL_PROXY=PROXY, NO_PROXY=".sii.edu.cn", no_proxy=".sii.edu.cn"):
            self.assertEqual(
                cfg.get_proxy(TARGET),
                "",
                "NO_PROXY 覆盖了 qz.sii.edu.cn，却仍然返回代理 —— "
                "这正是 2026-08-10 看板故障里 no_proxy 完全失效的原因",
            )

    def test_matches_what_requests_itself_decides(self):
        """和 requests 的判断对齐 —— 它是对的，我们不该有自己一套。"""
        with self._env(ALL_PROXY=PROXY, NO_PROXY=".sii.edu.cn", no_proxy=".sii.edu.cn"):
            requests_direct = not requests.utils.get_environ_proxies(TARGET)
            qzcli_direct = not cfg.get_proxy(TARGET)
            self.assertEqual(qzcli_direct, requests_direct)

    def test_proxy_still_used_for_hosts_not_in_no_proxy(self):
        """别矫枉过正 —— 不在 NO_PROXY 里的域名照样要走代理。"""
        with self._env(ALL_PROXY=PROXY, NO_PROXY=".sii.edu.cn", no_proxy=".sii.edu.cn"):
            self.assertEqual(cfg.get_proxy("https://example.com/x"), PROXY)

    def test_no_url_keeps_old_behaviour(self):
        """不传 url 时行为不变 —— 老调用点不能被这次改动影响。"""
        with self._env(ALL_PROXY=PROXY, NO_PROXY=".sii.edu.cn", no_proxy=".sii.edu.cn"):
            self.assertEqual(cfg.get_proxy(), PROXY)

    def test_no_proxy_unset_means_proxy_as_before(self):
        with self._env(ALL_PROXY=PROXY):
            self.assertEqual(cfg.get_proxy(TARGET), PROXY)

    def test_config_file_proxy_also_respects_no_proxy(self):
        """配置文件里配的代理同样受 NO_PROXY 约束。

        用户写了 no_proxy 就是在说「这个域名别走代理」，
        跟代理是从配置读的还是从环境读的无关。
        """
        self._p.stop()
        with patch.object(cfg, "load_config", return_value={"proxy": PROXY}):
            with self._env(NO_PROXY=".sii.edu.cn", no_proxy=".sii.edu.cn"):
                self.assertEqual(cfg.get_proxy(TARGET), "")
                self.assertEqual(cfg.get_proxy("https://example.com/"), PROXY)
        self._p.start()


class ApiCallSitesPassUrlTests(unittest.TestCase):
    """三个调用点都必须把目标 URL 传下去，否则 NO_PROXY 判断无从谈起。"""

    def test_all_get_proxy_call_sites_pass_a_url(self):
        import pathlib
        import re

        src = (
            pathlib.Path(__file__).resolve().parent.parent / "qzcli" / "api.py"
        ).read_text(encoding="utf-8")
        bare = re.findall(r"get_proxy\(\s*\)", src)
        self.assertEqual(
            bare,
            [],
            f"api.py 里还有 {len(bare)} 处 get_proxy() 没传 URL —— "
            "不传就绕不过 NO_PROXY，等于这次修复对那条路径无效",
        )


if __name__ == "__main__":
    unittest.main()
