"""Tests for proxy resolution and pool-manager scheme dispatch."""

import os
import unittest
from unittest.mock import patch

from qzcli import api, config


class GetProxyResolutionTests(unittest.TestCase):
    """`config.get_proxy()` precedence: config.json > ALL_PROXY > HTTPS_PROXY."""

    def setUp(self):
        # Clean env so each test only sees what it sets explicitly.
        self._saved_env = {
            k: os.environ.pop(k, None) for k in ("ALL_PROXY", "HTTPS_PROXY")
        }

    def tearDown(self):
        for k, v in self._saved_env.items():
            if v is not None:
                os.environ[k] = v
            else:
                os.environ.pop(k, None)

    def test_returns_empty_when_unset(self):
        with patch.object(config, "load_config", return_value={"proxy": ""}):
            self.assertEqual(config.get_proxy(), "")

    def test_config_json_wins_over_env(self):
        os.environ["ALL_PROXY"] = "socks5://from-env:1080"
        os.environ["HTTPS_PROXY"] = "http://from-env:8080"
        with patch.object(
            config, "load_config", return_value={"proxy": "socks5://from-cfg:7897"}
        ):
            self.assertEqual(config.get_proxy(), "socks5://from-cfg:7897")

    def test_all_proxy_env_fallback(self):
        os.environ["ALL_PROXY"] = "socks5://env-all:1080"
        os.environ["HTTPS_PROXY"] = "http://env-https:8080"
        with patch.object(config, "load_config", return_value={"proxy": ""}):
            self.assertEqual(config.get_proxy(), "socks5://env-all:1080")

    def test_https_proxy_used_when_no_all_proxy(self):
        os.environ["HTTPS_PROXY"] = "http://only-https:8080"
        with patch.object(config, "load_config", return_value={"proxy": ""}):
            self.assertEqual(config.get_proxy(), "http://only-https:8080")

    def test_returns_http_scheme_unchanged(self):
        # Non-SOCKS env values must be returned as-is, not silently dropped.
        with patch.object(
            config, "load_config", return_value={"proxy": "http://corp:3128"}
        ):
            self.assertEqual(config.get_proxy(), "http://corp:3128")


class GetPoolManagerSchemeTests(unittest.TestCase):
    """`api._get_pool_manager()` picks the right urllib3 manager per scheme."""

    def setUp(self):
        # Pool manager is lru_cached; clear so fresh instances are returned.
        api._get_pool_manager.cache_clear()

    def tearDown(self):
        api._get_pool_manager.cache_clear()

    def test_no_proxy_returns_plain_pool_manager(self):
        import urllib3

        pm = api._get_pool_manager("")
        self.assertIsInstance(pm, urllib3.PoolManager)
        # Must not be a ProxyManager subclass.
        self.assertNotIsInstance(pm, urllib3.ProxyManager)

    def test_http_proxy_returns_proxy_manager(self):
        import urllib3

        pm = api._get_pool_manager("http://corp.example:3128")
        self.assertIsInstance(pm, urllib3.ProxyManager)

    def test_https_proxy_returns_proxy_manager(self):
        import urllib3

        pm = api._get_pool_manager("https://corp.example:3128")
        self.assertIsInstance(pm, urllib3.ProxyManager)

    def test_socks5_proxy_returns_socks_proxy_manager(self):
        # Skip if PySocks isn't installed in the test env (matches runtime behavior).
        try:
            from urllib3.contrib.socks import SOCKSProxyManager
        except ImportError:
            self.skipTest("PySocks not installed; SOCKS support unavailable")

        pm = api._get_pool_manager("socks5://127.0.0.1:7897")
        self.assertIsInstance(pm, SOCKSProxyManager)

    def test_socks5h_proxy_returns_socks_proxy_manager(self):
        try:
            from urllib3.contrib.socks import SOCKSProxyManager
        except ImportError:
            self.skipTest("PySocks not installed; SOCKS support unavailable")

        pm = api._get_pool_manager("socks5h://127.0.0.1:7897")
        self.assertIsInstance(pm, SOCKSProxyManager)

    def test_unknown_scheme_raises(self):
        with self.assertRaises(api.QzAPIError):
            api._get_pool_manager("ftp://nope:21")

    def test_pool_manager_is_cached(self):
        # Same proxy string → same manager instance (connection-pool reuse).
        pm1 = api._get_pool_manager("http://corp.example:3128")
        pm2 = api._get_pool_manager("http://corp.example:3128")
        self.assertIs(pm1, pm2)
        # Different proxy → different manager.
        pm3 = api._get_pool_manager("http://other.example:3128")
        self.assertIsNot(pm1, pm3)

    def test_socks_import_error_raises_qz_error(self):
        # Simulate PySocks missing: importing urllib3.contrib.socks fails.
        import builtins

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "urllib3.contrib.socks":
                raise ImportError("No module named 'socks'")
            return real_import(name, *args, **kwargs)

        api._get_pool_manager.cache_clear()
        with patch.object(builtins, "__import__", side_effect=fake_import):
            with self.assertRaises(api.QzAPIError) as ctx:
                api._get_pool_manager("socks5://127.0.0.1:7897")
            self.assertIn("PySocks", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
