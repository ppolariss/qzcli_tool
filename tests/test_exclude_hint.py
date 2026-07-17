"""碎卡 → --exclude-node 参数桥(format_exclude_args / fragmented_node_names)。"""
import unittest

from qzcli import fragmentation as frag


def _node(total, used, lcg="L"):
    return {"lcg": lcg, "gpu_type": "H200", "cluster": "c",
            "gpu_total": total, "gpu_used": used}


class FormatExcludeArgsTests(unittest.TestCase):
    def test_dedup_strip_sort(self):
        s = frag.format_exclude_args([" gpu-b ", "gpu-a", "gpu-b", "", "  "])
        self.assertEqual(s, "--exclude-node gpu-a --exclude-node gpu-b")

    def test_empty_gives_empty_string(self):
        self.assertEqual(frag.format_exclude_args([]), "")
        self.assertEqual(frag.format_exclude_args([" ", None]), "")

    def test_pasteable_into_create(self):
        # 生成的串直接接在 create 后面能被 argparse 解析成 exclude_node 列表
        import argparse
        s = frag.format_exclude_args(["n1", "n2"])
        p = argparse.ArgumentParser()
        p.add_argument("--exclude-node", dest="exclude_node", action="append")
        ns = p.parse_args(s.split())
        self.assertEqual(ns.exclude_node, ["n1", "n2"])


class FragmentedNodeNamesTests(unittest.TestCase):
    def setUp(self):
        # A 空整 | B 低优满占 | C 碎卡满(无空) | D 碎卡有空 | E 高优满占
        nm = {"nA": _node(8, 0), "nB": _node(8, 8), "nC": _node(8, 8),
              "nD": _node(8, 5), "nE": _node(8, 8)}
        rows = [
            {"优先级": 2, "GPU": 8, "节点": "nB"},   # B 全低优 → 低优满占
            {"优先级": 2, "GPU": 3, "节点": "nC"},   # C 3低优
            {"优先级": 7, "GPU": 5, "节点": "nC"},   # C 5高优 → 混合满 = 碎卡(无空)
            {"优先级": 7, "GPU": 5, "节点": "nD"},   # D 5高优,3空 → 碎卡(有空)
            {"优先级": 9, "GPU": 8, "节点": "nE"},   # E 全高优 → 高优满占
        ]
        self.res = frag.compute_node_fragmentation(nm, rows)

    def test_all_fragmented(self):
        self.assertEqual(sorted(frag.fragmented_node_names(self.res)), ["nC", "nD"])

    def test_only_free_fragmented(self):
        # 只挑还有空卡的碎卡节点 → 仅 nD
        self.assertEqual(frag.fragmented_node_names(self.res, only_free=True), ["nD"])

    def test_lcg_filter(self):
        self.assertEqual(frag.fragmented_node_names(self.res, lcg="L"),
                         frag.fragmented_node_names(self.res))
        self.assertEqual(frag.fragmented_node_names(self.res, lcg="OTHER"), [])

    def test_end_to_end_exclude_string(self):
        names = frag.fragmented_node_names(self.res, only_free=True)
        self.assertEqual(frag.format_exclude_args(names), "--exclude-node nD")


if __name__ == "__main__":
    unittest.main()
