"""qzcli.fragmentation 纯函数单测(确定性,不连集群)。"""
import unittest

from qzcli import fragmentation as frag


def _node(total, used, lcg="L", gtype="H200", cluster="c"):
    return {"lcg": lcg, "gpu_type": gtype, "cluster": cluster,
            "gpu_total": total, "gpu_used": used}


class NodeLowPriorityTests(unittest.TestCase):
    def test_multi_node_even_split(self):
        rows = [{"优先级": 1, "GPU": 4, "节点": "nF,nG"}]
        low = frag.node_low_priority_gpu(rows)
        self.assertEqual(low, {"nF": 2, "nG": 2})

    def test_single_node_full_gpu(self):
        rows = [{"优先级": 2, "GPU": 8, "节点": "nB"}]
        self.assertEqual(frag.node_low_priority_gpu(rows)["nB"], 8)

    def test_high_priority_ignored(self):
        rows = [{"优先级": 6, "GPU": 8, "节点": "nE"}]
        self.assertEqual(frag.node_low_priority_gpu(rows), {})

    def test_threshold_boundary(self):
        rows = [{"优先级": 3, "GPU": 1, "节点": "n"},   # <=3 counts
                {"优先级": 4, "GPU": 1, "节点": "n"}]   # >3 ignored
        self.assertEqual(frag.node_low_priority_gpu(rows), {"n": 1})


class FragmentationTests(unittest.TestCase):
    def setUp(self):
        # A 空 | B 低优满占 | C 高低优混合满 | D 部分空 | E 高优满占 | F/G 多节点低优碎片
        self.node_map = {
            "nA": _node(8, 0),
            "nB": _node(8, 8),
            "nC": _node(8, 8),
            "nD": _node(8, 5),
            "nE": _node(8, 8),
            "nF": _node(8, 2),
            "nG": _node(8, 2),
        }
        self.rows = [
            {"优先级": 2, "GPU": 8, "节点": "nB"},          # B 全低优
            {"优先级": 3, "GPU": 3, "节点": "nC"},          # C 3 低优
            {"优先级": 7, "GPU": 5, "节点": "nC"},          # C 5 高优
            {"优先级": 8, "GPU": 5, "节点": "nD"},          # D 5 高优(3 空)
            {"优先级": 9, "GPU": 8, "节点": "nE"},          # E 全高优
            {"优先级": 1, "GPU": 4, "节点": "nF,nG"},        # 多节点低优 → 各 2
        ]
        self.res = frag.compute_node_fragmentation(self.node_map, self.rows)

    def test_per_node_class(self):
        cls = {n["node"]: n["class"] for n in self.res["nodes"]}
        self.assertEqual(cls["nA"], frag.EMPTY_WHOLE)
        self.assertEqual(cls["nB"], frag.LOWPRI_WHOLE)
        self.assertEqual(cls["nC"], frag.FRAGMENTED)   # 高低优混合满 → 碎
        self.assertEqual(cls["nD"], frag.FRAGMENTED)   # 有空卡 → 碎
        self.assertEqual(cls["nE"], frag.HI_WHOLE)
        self.assertEqual(cls["nF"], frag.FRAGMENTED)
        self.assertEqual(cls["nG"], frag.FRAGMENTED)

    def test_per_node_low_high_split(self):
        byname = {n["node"]: n for n in self.res["nodes"]}
        self.assertEqual((byname["nC"]["low_pri"], byname["nC"]["high_pri"]), (3, 5))
        self.assertEqual((byname["nF"]["low_pri"], byname["nF"]["free"]), (2, 6))

    def test_lcg_aggregate(self):
        a = self.res["by_lcg"]["L"]
        self.assertEqual(a["total_nodes"], 7)
        self.assertEqual(a["empty_whole"], 1)          # A
        self.assertEqual(a["lowpri_whole"], 1)         # B
        self.assertEqual(a["hi_whole"], 1)             # E
        self.assertEqual(a["frag_nodes"], 4)           # C D F G
        self.assertEqual(a["frag_lowpri_cards"], 3 + 2 + 2)   # C + F + G = 7
        self.assertEqual(a["frag_free_cards"], 3 + 6 + 6)     # D + F + G = 15
        self.assertEqual(a["free_total"], 8 + 3 + 6 + 6)      # A D F G = 23
        self.assertEqual(a["node_size"], 8)
        # 可凑整节点潜力 = (空卡 23 + 碎片低优 7) // 8
        self.assertEqual(a["whole_node_potential"], (23 + 7) // 8)

    def test_low_pri_capped_at_used(self):
        # 均摊可能让低优 > 已用;不应超过已用
        nm = {"n": _node(8, 2)}
        rows = [{"优先级": 1, "GPU": 8, "节点": "n"}]   # 摊 8 但只用了 2
        r = frag.compute_node_fragmentation(nm, rows)
        node = r["nodes"][0]
        self.assertEqual(node["low_pri"], 2)           # capped
        self.assertEqual(node["high_pri"], 0)

    def test_abnormal_zero_total_node_skipped(self):
        nm = {"bad": _node(0, 4)}      # gpu_total=0 异常节点
        r = frag.compute_node_fragmentation(nm, [])
        self.assertEqual(r["by_lcg"]["L"]["total_nodes"], 0)


class SchedulingDisabledTests(unittest.TestCase):
    """SchedulingDisabled / cordon 节点即便有空卡也调度不上去,必须从可用/碎卡剔除。"""

    def _nm(self, **over):
        # nA 空整(可调度) | nS 调度禁用但有空卡 | nS2 调度禁用整空
        nm = {
            "nA": {**_node(8, 0), "schedulable": True},
            "nS": {**_node(8, 3), "schedulable": False,
                   "status": "SchedulingDisabled", "cordon_type": "machine-migration"},
            "nS2": {**_node(8, 0), "schedulable": False,
                    "status": "SchedulingDisabled", "cordon_type": "sopa-software-fault"},
        }
        nm.update(over)
        return nm

    def test_disabled_node_classified_sched_disabled(self):
        res = frag.compute_node_fragmentation(self._nm(), [])
        cls = {n["node"]: n["class"] for n in res["nodes"]}
        self.assertEqual(cls["nS"], frag.SCHED_DISABLED)
        self.assertEqual(cls["nS2"], frag.SCHED_DISABLED)
        self.assertEqual(cls["nA"], frag.EMPTY_WHOLE)

    def test_disabled_excluded_from_available_counts(self):
        a = frag.compute_node_fragmentation(self._nm(), [])["by_lcg"]["L"]
        self.assertEqual(a["total_nodes"], 3)
        self.assertEqual(a["sched_disabled_nodes"], 2)
        # nS: total8 used3 → free5; nS2: total8 used0 → free8 → 合计 13
        self.assertEqual(a["sched_disabled_free_gpus"], 5 + 8)
        # 只有 nA 计入可用:空整1、free_total=8、碎片0
        self.assertEqual(a["empty_whole"], 1)
        self.assertEqual(a["frag_nodes"], 0)
        self.assertEqual(a["free_total"], 8)          # 不含调度禁用的 13
        self.assertEqual(a["frag_free_cards"], 0)
        # 可凑潜力 = free_total(8) // node_size(8) = 1,不被 13 张幽灵空卡抬高
        self.assertEqual(a["whole_node_potential"], 1)

    def test_disabled_not_in_exclude_string(self):
        res = frag.compute_node_fragmentation(self._nm(), [])
        names = frag.fragmented_node_names(res)
        self.assertNotIn("nS", names)
        self.assertNotIn("nS2", names)

    def test_missing_schedulable_defaults_true(self):
        # 老数据/远程 collector 不带 schedulable → 当可调度(向后兼容)
        nm = {"n": _node(8, 0)}   # 无 schedulable 键
        res = frag.compute_node_fragmentation(nm, [])
        self.assertEqual(res["nodes"][0]["class"], frag.EMPTY_WHOLE)
        self.assertEqual(res["by_lcg"]["L"]["sched_disabled_nodes"], 0)


if __name__ == "__main__":
    unittest.main()
