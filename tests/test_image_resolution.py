"""镜像解析的优先级：显式 > 平台 > 历史 > 明确报错。

## 起因是一个打到所有用户的 bug

`qzcli create` 的默认值曾经是：

    DEFAULT_CREATE_IMAGE      = ".../dhyu-wan-torch29:0.4"   # 已从平台删除
    DEFAULT_CREATE_IMAGE_TYPE = "SOURCE_PRIVATE"             # 但镜像在公共 registry

二分实测（2026-08-12，同一个 payload 只换这一个字段）：

    image_type=SOURCE_PRIVATE → ✗ InternalError: Unauthorized
    image_type=SOURCE_PUBLIC  → ✓ 成功

`SOURCE_PRIVATE` 会让后端拿镜像去私有 registry 鉴权，401 被包成外层
`InternalError`。**任何不指定镜像的用户都必然踩到**，而且报出来的错完全指不到
镜像上 —— 交互式创建时两个都是默认值，一路回车就中招。

冒烟测试只是碰巧当了报信的：它连红三轮，我先后怀疑过代理改动、macOS 系统代理
排除表、会话鉴权、项目/计算组错配、镜像本身，全都被证据排除，最后二分到
`image_type`。

## 这个文件钉什么

**第一优先级是用户控制权**：显式传的绝不能被任何推断覆盖。
这条我自己犯过 —— 在 `tools/live_smoke.py` 里让历史值盖掉了 `--image`。
"""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from qzcli import cli  # noqa: E402

IMG = "registry.example.invalid/team/train:1"
OTHER = "registry.example.invalid/team/other:2"


class _Api:
    """只实现解析器会碰的那几个方法。"""

    def __init__(self, images=None, jobs=None, details=None):
        self._images = images or []
        self._jobs = jobs or []
        self._details = details or {}

    def _request_v2(self, service, action, body, **kw):
        return {"images": self._images}

    def list_jobs_with_cookie(self, ws, cookie, page_size=100):
        return {"jobs": self._jobs}

    def _get_job_detail_v2(self, job_id, cookie):
        return self._details.get(job_id, {})


def _job(jid, status="job_succeeded", name="real-job"):
    return {"job_id": jid, "status": status, "name": name}


def _detail(image, itype):
    return {"framework_config": [{"image": image, "image_type": itype}]}


class ExplicitAlwaysWins(unittest.TestCase):
    """第 1 档 —— 这条比其它三条加起来都重要。"""

    def test_both_given_are_returned_untouched(self):
        api = _Api(
            images=[{"image_url": IMG, "visibility": "VISIBILITY_PUBLIC"}],
            jobs=[_job("j1")],
            details={"j1": _detail(OTHER, "SOURCE_PRIVATE")},
        )
        got = cli.resolve_create_image(api, "ck", "ws-1", IMG, "SOURCE_PRIVATE")
        self.assertEqual(
            (IMG, "SOURCE_PRIVATE"),
            got,
            "用户显式传了镜像和类型，却被平台/历史的值覆盖了 —— "
            "这是拿走用户对自己命令的控制权",
        )

    def test_explicit_survives_even_when_platform_disagrees(self):
        """平台说这镜像是 PUBLIC，用户偏要 PRIVATE —— 听用户的。"""
        api = _Api(images=[{"image_url": IMG, "visibility": "VISIBILITY_PUBLIC"}])
        self.assertEqual(
            (IMG, "SOURCE_PRIVATE"),
            cli.resolve_create_image(api, "ck", "ws-1", IMG, "SOURCE_PRIVATE"),
        )


class TypeFromPlatform(unittest.TestCase):
    """第 2 档：有镜像没类型 → 问平台，不猜。"""

    def test_public_visibility_maps_to_source_public(self):
        api = _Api(images=[{"image_url": IMG, "visibility": "VISIBILITY_PUBLIC"}])
        self.assertEqual(
            (IMG, "SOURCE_PUBLIC"),
            cli.resolve_create_image(api, "ck", "ws-1", IMG, None),
        )

    def test_private_visibility_maps_to_source_private(self):
        api = _Api(images=[{"image_url": IMG, "visibility": "VISIBILITY_PRIVATE"}])
        self.assertEqual(
            (IMG, "SOURCE_PRIVATE"),
            cli.resolve_create_image(api, "ck", "ws-1", IMG, None),
        )

    def test_platform_error_does_not_break_create(self):
        """查可见性只是加分项，它挂了要能落到下一档，不能拖垮 create。"""

        class _Broken(_Api):
            def _request_v2(self, *a, **kw):
                raise RuntimeError("image 服务不可用")

        api = _Broken(jobs=[_job("j1")], details={"j1": _detail(IMG, "SOURCE_PUBLIC")})
        self.assertEqual(
            (IMG, "SOURCE_PUBLIC"),
            cli.resolve_create_image(api, "ck", "ws-1", IMG, None),
        )


class TypeAndImageFromHistory(unittest.TestCase):
    """第 3 档：退回用户自己近期真实跑过的任务。"""

    def test_picks_image_and_type_when_nothing_given(self):
        api = _Api(jobs=[_job("j1")], details={"j1": _detail(IMG, "SOURCE_PUBLIC")})
        self.assertEqual(
            (IMG, "SOURCE_PUBLIC"),
            cli.resolve_create_image(api, "ck", "ws-1", None, None),
        )

    def test_skips_create_failed_and_own_smoke_jobs(self):
        """别拿坏样本当参考 —— 创建就失败的任务，镜像多半正是坏的那个。

        2026-08-12 的实际现场：最近 30 个任务里 26 个是冒烟工具留下的
        job_create_failed，全都带着那个已失效的镜像。
        """
        api = _Api(
            jobs=[
                _job("bad", status="job_create_failed", name="someone-else"),
                _job("mine", status="job_succeeded", name="qzcli-v2-smoke-1"),
                _job("good", status="job_succeeded", name="real-training"),
            ],
            details={
                "bad": _detail("registry.example.invalid/dead:0", "SOURCE_PRIVATE"),
                "mine": _detail("registry.example.invalid/smoke:0", "SOURCE_PRIVATE"),
                "good": _detail(IMG, "SOURCE_PUBLIC"),
            },
        )
        self.assertEqual(
            (IMG, "SOURCE_PUBLIC"),
            cli.resolve_create_image(api, "ck", "ws-1", None, None),
        )


class ClearErrorInsteadOfDoomedDefault(unittest.TestCase):
    """第 4 档：说清要传什么，而不是拿必然失败的默认值去撞。"""

    def test_nothing_available_raises_with_actionable_message(self):
        api = _Api()
        with self.assertRaises(cli._ImageResolutionError) as ctx:
            cli.resolve_create_image(api, "ck", "ws-1", None, None)
        msg = str(ctx.exception)
        self.assertIn("--image", msg)
        self.assertIn("--image-type", msg)

    def test_image_without_determinable_type_raises(self):
        api = _Api()
        with self.assertRaises(cli._ImageResolutionError) as ctx:
            cli.resolve_create_image(api, "ck", "ws-1", IMG, None)
        msg = str(ctx.exception)
        self.assertIn("SOURCE_PUBLIC", msg)
        self.assertIn(
            "Unauthorized",
            msg,
            "报错里要点出这个症状 —— 否则用户下次撞到 InternalError: Unauthorized "
            "还是想不到是镜像类型的问题",
        )

    def test_does_not_silently_fall_back_to_the_dead_default(self):
        """最关键的一条：绝不能再回到「套用写死默认值」的老路。"""
        api = _Api()
        with self.assertRaises(cli._ImageResolutionError):
            cli.resolve_create_image(api, "ck", "ws-1", None, None)


if __name__ == "__main__":
    unittest.main()
