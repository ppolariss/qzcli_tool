"""CAS 登录失败时报出来的原因必须是真的。

## 背景

用户报「``qzcli login`` 用正确密码却说需要输入验证码」，跑去浏览器一看根本没有
验证码可过。查下来判据是：

    if "验证码" in resp.text:
        raise QzAPIError("需要输入验证码，请在浏览器中登录后手动获取 cookie")

而 CAS 登录页 **永远** 含"验证码"三个字 —— 抓下来数过，整整 5 处，全部来自旁边
那个「短信验证码登录」标签页的固定文案。其中那个图形验证码 ``<img>`` 指向的还是
``mapp.suda.edu.cn``（苏州大学），是模板里没清干净的死代码。

于是**任何**退回登录页的失败都被翻译成"需要输入验证码"。真实原因（多半是短时间
内登录过于频繁被 CAS 挡回）完全没被说出来，反而诱导用户反复重试 —— 而重试正是
让情况变糟的动作。

下面 fixture 里那 5 处"验证码"是照抄真实页面的，就是为了钉死这个误报。
"""

import unittest

from qzcli.api import _describe_cas_login_failure

#: 真实登录页里那 5 处"验证码"，一字不改地抄下来。任何判据都不能被它们触发。
_BENIGN_SMS_TAB_MARKUP = """
<li class="auth_ul_li2"><h3>验证码登录</h3></li>
<input class="auth_input paw_input" type="text" placeholder="验证码">
<img src="http://mapp.suda.edu.cn/_control/validateimage?tt=0.226">
<!-- 验证码登录 -->
<span id="send_button" class="sendMsg">发送验证码</span>
<input autocomplete="off" type="text" class="auth_input" placeholder="动态验证码"/>
"""


def _page(error_html=""):
    """拼一个逼近真实结构的登录页：短信标签页固定文案 + 可选的错误容器。"""
    return f"""<html><body>
    {_BENIGN_SMS_TAB_MARKUP}
    <form><div class="form-error">{error_html}</div></form>
    </body></html>"""


class CasLoginFailureMessageTests(unittest.TestCase):
    def test_benign_captcha_markup_is_not_reported_as_captcha(self):
        """核心回归钉子：页面自带的"验证码"文案不能被当成"需要验证码"。

        这条在修复前必红 —— 那正是用户踩到的。
        """
        message = _describe_cas_login_failure(_page())
        self.assertNotIn("要求验证码", message)
        self.assertNotIn("需要输入验证码", message)

    def test_no_error_text_says_so_honestly(self):
        """页面没给原因时要如实说读不到，并给出可执行的下一步 —— 不许编。"""
        message = _describe_cas_login_failure(_page())
        self.assertIn("未给出具体原因", message)
        self.assertIn("qzcli cookie", message, "要告诉用户手工设 cookie 这条路")
        self.assertIn("频繁", message, "要点出最常见的真实原因")

    def test_real_captcha_error_is_reported(self):
        """真的要验证码时（错误容器里明确写了）必须报出来。"""
        message = _describe_cas_login_failure(_page("验证码错误，请重新输入"))
        self.assertIn("验证码", message)
        self.assertIn("验证码错误，请重新输入", message, "原文要带上")

    def test_password_error_is_reported(self):
        message = _describe_cas_login_failure(_page("用户名或密码错误"))
        self.assertIn("用户名或密码错误", message)

    def test_account_locked_error_is_passed_through(self):
        """没预料到的错误也要原样透出，而不是套一句泛化文案。

        以前这条会掉进"请检查用户名和密码"，把真正的原因盖掉。
        """
        message = _describe_cas_login_failure(_page("账号已被锁定，请 30 分钟后重试"))
        self.assertIn("账号已被锁定，请 30 分钟后重试", message)

    def test_html_tags_inside_error_are_stripped(self):
        message = _describe_cas_login_failure(
            _page("<span>登录过于频繁</span><br/>请稍后再试")
        )
        self.assertNotIn("<span>", message)
        self.assertIn("登录过于频繁", message)

    def test_empty_and_garbage_input_do_not_crash(self):
        for bad in ("", None, "not html at all", "<html>"):
            with self.subTest(html=bad):
                self.assertTrue(_describe_cas_login_failure(bad))


if __name__ == "__main__":
    unittest.main()
