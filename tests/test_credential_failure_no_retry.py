"""凭据类登录失败**绝不自动重试**。

## 这条是拿一个被锁的账号换来的

2026-08-12，`~/.qzcli/.relogin.cooldown` 里留下这句：

    用户名或密码错误：您的账号被锁定，请联系管理员。

成因是 `_recent_relogin_failure` 对**所有**失败一律 60 秒冷却 —— 密码不对和
网络抖一下被当成同一类。于是密码一旦失效，qzcli 每分钟自动送一次错误密码，
攒够次数 CAS 把账号锁死。

这两种失败的正确处理是**相反**的：

- 临时被挡（频繁登录触发限流/验证码）：等几分钟自己好，可以重试
- 凭据不对（密码错 / 账号被锁）：越重试锁得越死，必须停手

代码里本来就有行注释写着「真实踩过：一轮压测里几十个子进程并发触发，账号被锁到
要人工过验证码」—— 当时的对策是加锁 + 60 秒冷却，对凭据类错误根本不够。
"""

import os
import sys
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qzcli import api  # noqa: E402


class CredentialFailureClassifierTests(unittest.TestCase):
    def test_recognises_real_lockout_message(self):
        """用**真实发生过**的那句文案做判据，不是我想象的形状。"""
        self.assertTrue(
            api.is_credential_failure(
                "用户名或密码错误：您的账号被锁定，请联系管理员。"
            )
        )

    def test_recognises_plain_wrong_password(self):
        self.assertTrue(api.is_credential_failure("用户名或密码错误：密码错误"))

    def test_transient_failures_are_not_credential_failures(self):
        """临时被挡不能被误判成凭据错误 —— 否则会永久停掉自动登录。"""
        for msg in (
            "CAS 暂时要求验证码：验证码信息无效（短时间内登录过于频繁会触发）",
            "登录失败：CAS 把请求退回了登录页，但页面未给出具体原因。",
            "无法连接到启智平台",
            "",
        ):
            self.assertFalse(api.is_credential_failure(msg), msg[:24])


class NoAutoRetryAfterCredentialFailureTests(unittest.TestCase):
    def setUp(self):
        api._clear_relogin_failure()
        self.addCleanup(api._clear_relogin_failure)

    def test_credential_failure_blocks_relogin_forever(self):
        """不是冷却 —— 是一直挡着，直到用户自己登录成功。"""
        api._record_relogin_failure("用户名或密码错误：您的账号被锁定，请联系管理员。")
        # 把时间推到远超 60 秒冷却之后
        with patch.object(api._time, "time", return_value=time.time() + 86400):
            blocked = api._recent_relogin_failure()
        self.assertIsNotNone(
            blocked,
            "账号被锁之后 60 秒又放行自动登录 —— 这正是把账号锁死的那条路径",
        )
        self.assertIn("锁定", blocked)

    def test_transient_failure_still_expires_after_cooldown(self):
        """别矫枉过正：临时失败过了冷却期就该允许重试。"""
        api._record_relogin_failure("CAS 暂时要求验证码：验证码信息无效")
        with patch.object(api._time, "time", return_value=time.time() + 3600):
            self.assertIsNone(
                api._recent_relogin_failure(),
                "临时失败被永久挡住了，用户会以为工具坏了",
            )

    def test_successful_login_clears_the_block(self):
        api._record_relogin_failure("用户名或密码错误：您的账号被锁定")
        self.assertIsNotNone(api._recent_relogin_failure())
        api._clear_relogin_failure()
        self.assertIsNone(
            api._recent_relogin_failure(), "登录成功后必须能解除封锁，否则永远用不了"
        )


if __name__ == "__main__":
    unittest.main()
