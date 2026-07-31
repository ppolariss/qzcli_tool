"""缓存残缺态 × 读缓存的判定函数 —— 矩阵覆盖。

## 这组测试要钉住的契约

读缓存的校验函数是**三态**的：

- ``True``  —— 缓存能确定"属于"
- ``False`` —— 缓存能确定"**不**属于"
- ``None``  —— **缓存无从判断**，调用方应放行去向平台复核

线上那批 bug 全是同一个形状：**该返回 ``None`` 的时候返回了 ``False``**。
缓存里没有 ≠ 平台上没有 —— 新建的计算组、新加入的项目都不在缓存里，于是用户
被告知"计算组不属于当前工作空间"，而它明明属于。

所以这里的断言口径不是"返回 1"或"抛异常"，而是：

    **残缺的缓存只能产生 ``None``（不知道），绝不能产生 ``False``（确定没有）。**

顺带钉住"不许崩"：损坏的 JSON、缺 ``id`` 的条目、值不是 dict 的工作空间，
都不该让整条命令挂掉 —— 这类异常一旦被宽泛的 except 吞掉，表现就是整个工作空间
凭空消失，比报错更难查。
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from support import cache_states as cs  # noqa: E402
from support.sandbox import sandbox_home  # noqa: E402

from qzcli import cli  # noqa: E402

#: 缓存里"确实没有"目标资源的状态。对这些，函数只能说"不知道"，不能说"没有"。
_DEGRADED_STATES = [
    "missing",
    "empty",
    "no-projects",
    "no-specs",
    "no-compute-groups",
    "new-compute-group",
    "new-project",
    "item-missing-id",
    "workspace-not-a-dict",
    "corrupt-json",
]


class MembershipNeverFalseNegativeTests(unittest.TestCase):
    """归属校验：缓存残缺时只能返回 None，不能返回 False。

    ``False`` 会让 ``create`` 直接拒绝提交并告诉用户"资源不属于这个工作空间" ——
    一句在缓存旧了的时候必然是假的话。``None`` 则让调用方去平台复核，那才是对的。
    """

    def test_compute_group_membership(self):
        for name in _DEGRADED_STATES:
            build, desc = cs.ALL_STATES[name]
            with self.subTest(state=name, desc=desc):
                with sandbox_home(resources=build()):
                    verdict = cli._validate_cached_resource_membership(
                        cs.WS_ID, "compute_groups", cs.LCG_ID
                    )
                self.assertIsNot(
                    verdict,
                    False,
                    f"[{name}] 缓存残缺却断言「计算组不属于本空间」—— 这是假错误",
                )

    def test_project_membership(self):
        """项目和计算组是同构代码。v0.4.2 只修了后者，项目那半留到 v0.4.3 才补。"""
        for name in _DEGRADED_STATES:
            build, desc = cs.ALL_STATES[name]
            with self.subTest(state=name, desc=desc):
                with sandbox_home(resources=build()):
                    verdict = cli._validate_cached_resource_membership(
                        cs.WS_ID, "projects", cs.PROJECT_ID
                    )
                self.assertIsNot(
                    verdict, False, f"[{name}] 缓存残缺却断言「项目不属于本空间」"
                )

    def test_spec_membership(self):
        for name in _DEGRADED_STATES:
            build, desc = cs.ALL_STATES[name]
            with self.subTest(state=name, desc=desc):
                with sandbox_home(resources=build()):
                    verdict = cli._validate_cached_spec_membership(
                        cs.WS_ID, cs.LCG_ID, cs.SPEC_ID
                    )
                self.assertIsNot(verdict, False, f"[{name}] 缓存残缺却断言规格不可用")

    def test_healthy_cache_still_answers_definitively(self):
        """对照组：缓存完整时必须给出确定答案，否则上面那些断言就没意义了。

        少了这条，把函数改成"永远返回 None"也能让全部用例变绿。
        """
        with sandbox_home(resources=cs.healthy()):
            self.assertIs(
                cli._validate_cached_resource_membership(
                    cs.WS_ID, "compute_groups", cs.LCG_ID
                ),
                True,
            )
            self.assertIs(
                cli._validate_cached_resource_membership(
                    cs.WS_ID, "projects", cs.PROJECT_ID
                ),
                True,
            )

    def test_healthy_cache_rejects_a_genuinely_foreign_id(self):
        """缓存完整且确实没有该资源时，``False`` 是对的 —— 这时才该拒绝。"""
        with sandbox_home(resources=cs.healthy()):
            self.assertIs(
                cli._validate_cached_resource_membership(
                    cs.WS_ID, "compute_groups", "lcg-不存在的组"
                ),
                False,
            )


class DegradedCacheDoesNotCrashTests(unittest.TestCase):
    """损坏的缓存不能把命令打崩。

    被吞掉的异常比报错更难查：表现是整个工作空间凭空消失，没有任何线索。
    """

    def test_load_all_resources_over_every_state(self):
        for name, (build, desc) in cs.ALL_STATES.items():
            with self.subTest(state=name, desc=desc):
                with sandbox_home(resources=build()):
                    from qzcli import config

                    result = config.load_all_resources()
                self.assertIsInstance(result, dict, f"[{name}] 应降级成 dict")

    def test_get_workspace_resources_over_every_state(self):
        for name, (build, desc) in cs.ALL_STATES.items():
            with self.subTest(state=name, desc=desc):
                with sandbox_home(resources=build()):
                    cli.get_workspace_resources(cs.WS_ID)  # 不抛即可


class SpecScopingTests(unittest.TestCase):
    """规格按计算组筛选 —— "规格凭空消失"的那条路。"""

    def test_spec_missing_lcg_ids_is_kept_when_only_one_group(self):
        """只有一个计算组时，缺归属字段的规格可以合理推断为属于它。"""
        spec = {"id": cs.SPEC_ID, "name": "8卡", "gpu_count": 8}
        scoped = cli._scope_specs_to_compute_group(
            [spec], cs.LCG_ID, {cs.LCG_ID: {"id": cs.LCG_ID}}
        )
        self.assertEqual(len(scoped), 1, "唯一计算组时不该丢弃")
        self.assertEqual(scoped[0]["logic_compute_group_ids"], [cs.LCG_ID])

    def test_spec_missing_lcg_ids_is_dropped_when_multiple_groups(self):
        """多个计算组时无法推断，只能丢弃 —— 但这是**静默**的。

        用户看到的是"没有可用规格"，完全无从排查规格去哪了。这里先把行为钉住；
        是否要补一条 warning 已记在待办里（C9）。
        """
        spec = {"id": cs.SPEC_ID, "name": "8卡", "gpu_count": 8}
        scoped = cli._scope_specs_to_compute_group(
            [spec],
            cs.LCG_ID,
            {cs.LCG_ID: {"id": cs.LCG_ID}, cs.OTHER_LCG_ID: {"id": cs.OTHER_LCG_ID}},
        )
        self.assertEqual(scoped, [], "多组且无归属字段时丢弃")

    def test_spec_belonging_to_another_group_is_filtered_out(self):
        ws = cs.partial_specs_other_lcg()[cs.WS_ID]
        scoped = cli._scope_specs_to_compute_group(
            list(ws["specs"].values()), cs.LCG_ID, ws["compute_groups"]
        )
        self.assertEqual(scoped, [], "别的计算组的规格不能串用")

    def test_empty_compute_group_id_keeps_everything(self):
        """没指定计算组时不筛 —— 否则会把所有规格都筛没。"""
        specs = [{"id": "a"}, {"id": "b"}]
        self.assertEqual(len(cli._scope_specs_to_compute_group(specs, "", {})), 2)


if __name__ == "__main__":
    unittest.main()
