"""Tests for cookie auto-relogin (P0) and CAS login retry-with-backoff (P1)."""

import argparse
import unittest
from unittest.mock import MagicMock, patch

from qzcli import api as qz_api
from qzcli.api import QzAPI, QzAPIError, QzTransientError, with_auth_retry
from qzcli.cli import _exec_via_jupyter, cmd_exec, cmd_exec_attach


class WithAuthRetryDecoratorTests(unittest.TestCase):
    """`with_auth_retry`: on 401, relogin once and retry with the fresh cookie."""

    class FakeAPI:
        def __init__(self, relogin_result="new-cookie", auto_relogin=True):
            self._auto_relogin = auto_relogin
            self._relogin_result = relogin_result
            self.relogin_calls = 0
            self.cookies_seen = []

        def _relogin(self):
            self.relogin_calls += 1
            return self._relogin_result

        @with_auth_retry
        def fetch(self, job_id, cookie):
            self.cookies_seen.append(cookie)
            if cookie != "new-cookie":
                raise QzAPIError("Cookie 已过期或无效", 401)
            return {"job": job_id}

        @with_auth_retry
        def no_cookie_arg(self):
            self.cookies_seen.append("call")
            if self.relogin_calls == 0:
                raise QzAPIError("Cookie 已过期", 401)
            return "ok"

    def test_retries_with_fresh_cookie(self):
        api = self.FakeAPI()
        result = api.fetch("j1", "stale-cookie")
        self.assertEqual(result, {"job": "j1"})
        self.assertEqual(api.relogin_calls, 1)
        # 第一次用旧 cookie 401，第二次换上 relogin 的新 cookie
        self.assertEqual(api.cookies_seen, ["stale-cookie", "new-cookie"])

    def test_no_cookie_arg_method_retries_after_relogin(self):
        api = self.FakeAPI()
        self.assertEqual(api.no_cookie_arg(), "ok")
        self.assertEqual(api.relogin_calls, 1)

    def test_reraises_when_relogin_unavailable(self):
        api = self.FakeAPI(relogin_result=None)
        with self.assertRaises(QzAPIError) as ctx:
            api.fetch("j1", "stale-cookie")
        self.assertEqual(ctx.exception.code, 401)
        self.assertEqual(api.relogin_calls, 1)

    def test_disabled_auto_relogin_is_noop(self):
        api = self.FakeAPI(auto_relogin=False)
        with self.assertRaises(QzAPIError):
            api.fetch("j1", "stale-cookie")
        self.assertEqual(api.relogin_calls, 0)

    def test_non_401_error_not_retried(self):
        @with_auth_retry
        def fails(self):
            raise QzAPIError("server exploded", 500)

        fake = self.FakeAPI()
        with self.assertRaises(QzAPIError) as ctx:
            fails(fake)
        self.assertEqual(ctx.exception.code, 500)
        self.assertEqual(fake.relogin_calls, 0)


class ReloginTests(unittest.TestCase):
    """`QzAPI._relogin`: persist a fresh cookie via CAS, or no-op without creds."""

    def test_relogin_logs_in_and_persists(self):
        api = QzAPI(username="u", password="p")
        with patch.object(
            api, "login_with_cas", return_value="fresh"
        ) as mock_login, patch.object(
            qz_api, "get_cookie", return_value={"cookie": "old", "workspace_id": "ws-1"}
        ), patch.object(
            qz_api, "save_cookie"
        ) as mock_save:
            result = api._relogin()
        self.assertEqual(result, "fresh")
        mock_login.assert_called_once_with("u", "p")
        mock_save.assert_called_once_with("fresh", "ws-1")

    def test_relogin_without_credentials_returns_none(self):
        api = QzAPI(username="", password="")
        api._username, api._password = "", ""
        with patch.object(api, "login_with_cas") as mock_login:
            self.assertIsNone(api._relogin())
        mock_login.assert_not_called()

    def test_relogin_returns_none_on_login_failure(self):
        api = QzAPI(username="u", password="p")
        with patch.object(
            api, "login_with_cas", side_effect=QzAPIError("nope")
        ), patch.object(
            qz_api, "get_cookie", return_value={"cookie": "old"}
        ), patch.object(
            qz_api, "save_cookie"
        ):
            self.assertIsNone(api._relogin())


class LoginRetryTests(unittest.TestCase):
    """`login_with_cas`: retry transient failures, propagate permanent ones."""

    def setUp(self):
        self.api = QzAPI(username="u", password="p")

    def test_retries_transient_then_succeeds(self):
        attempts = [QzTransientError("ssl eof"), QzTransientError("503"), "cookie"]

        def fake_once(username, password):
            item = attempts.pop(0)
            if isinstance(item, Exception):
                raise item
            return item

        with patch.object(
            self.api, "_login_with_cas_once", side_effect=fake_once
        ) as mock_once, patch.object(qz_api._time, "sleep") as mock_sleep:
            result = self.api.login_with_cas("u", "p")
        self.assertEqual(result, "cookie")
        self.assertEqual(mock_once.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_permanent_error_not_retried(self):
        with patch.object(
            self.api, "_login_with_cas_once", side_effect=QzAPIError("用户名或密码错误")
        ) as mock_once, patch.object(qz_api._time, "sleep"):
            with self.assertRaises(QzAPIError):
                self.api.login_with_cas("u", "p")
        mock_once.assert_called_once()

    def test_exhausts_retries_then_raises_transient(self):
        with patch.object(
            self.api, "_login_with_cas_once", side_effect=QzTransientError("ssl eof")
        ) as mock_once, patch.object(qz_api._time, "sleep"):
            with self.assertRaises(QzTransientError):
                self.api.login_with_cas("u", "p")
        self.assertEqual(mock_once.call_count, qz_api._LOGIN_MAX_TRIES)


class ExecDetachAttachTests(unittest.TestCase):
    """`exec --detach` launches without polling; `exec-attach` re-polls a job_id."""

    INFO = {"base_url": "u", "token": "t", "notebook_id": "nb-1"}

    def test_detach_launches_and_returns_job_id(self):
        args = argparse.Namespace(
            host="dev", remote_cmd=["sleep", "100"], timeout=120, detach=True
        )
        with patch(
            "qzcli.cli._find_notebook_jupyter_info", return_value=self.INFO
        ), patch(
            "qzcli.cli._exec_launch", return_value="qzcli_42"
        ) as mock_launch, patch(
            "qzcli.cli._exec_poll"
        ) as mock_poll, patch(
            "qzcli.cli.get_display", return_value=MagicMock()
        ):
            rc = cmd_exec(args)
        self.assertEqual(rc, 0)
        mock_launch.assert_called_once()
        mock_poll.assert_not_called()  # detach 不轮询

    def test_attach_polls_existing_job(self):
        args = argparse.Namespace(host="dev", job_id="qzcli_42", timeout=90)
        with patch(
            "qzcli.cli._find_notebook_jupyter_info", return_value=self.INFO
        ), patch(
            "qzcli.cli._exec_poll", return_value=(0, "done", True)
        ) as mock_poll, patch(
            "qzcli.cli.get_display", return_value=MagicMock()
        ):
            rc = cmd_exec_attach(args)
        self.assertEqual(rc, 0)
        self.assertEqual(mock_poll.call_args.args[1], "qzcli_42")

    def test_via_jupyter_timeout_prints_attach_hint(self):
        display = MagicMock()
        with patch("qzcli.cli._exec_launch", return_value="qzcli_7"), patch(
            "qzcli.cli._exec_poll", return_value=(124, "partial", False)
        ):
            exit_code, output = _exec_via_jupyter(self.INFO, "cmd", display, timeout=5)
        self.assertEqual((exit_code, output), (124, "partial"))
        warned = " ".join(str(c.args[0]) for c in display.print_warning.call_args_list)
        self.assertIn("exec-attach", warned)
        self.assertIn("qzcli_7", warned)


if __name__ == "__main__":
    unittest.main()
