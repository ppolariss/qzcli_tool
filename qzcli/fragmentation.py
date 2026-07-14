"""节点粒度的「整节点 / 碎卡」聚合（纯函数，无 IO）。

治理"碎卡"的核心量:GPU 卡分散在混合(高优+低优)节点上,凑不成整节点,导致多卡
job 排不上。`qzcli avail --lp` 只数"整节点 100% 被低优占"的节点(能整节点抢占),
把散在混合节点上的低优卡全丢了 —— 那部分就是**碎片低优卡**(可回收但凑不成整节点)。

本模块从看板/avail 都已经拿到的两份数据算出节点分类与每计算组的碎卡指标:
  - node_map:  {node_name: {lcg, gpu_type, cluster, gpu_total, gpu_used}}
               (来自 cli.build_node_to_lcg_map)
  - task_rows: [{"优先级": int, "节点": "hostA,hostB", "GPU": int, ...}]
               (来自 cli.task_dimension_to_row)

口径与 cmd_avail 一致(cli.py:1425-1438, 1578):低优 = 优先级<=阈值;多节点任务把
GPU 平均摊到各节点;"低优满占整节点" = 节点上低优卡数 >= 该节点总卡数。
"""
from __future__ import annotations

from collections import Counter, defaultdict

# 节点分类
EMPTY_WHOLE = "空整节点"      # 完全空闲,可直接排整节点
LOWPRI_WHOLE = "低优满占"     # 整节点被低优占满,可整节点抢占(= avail 的"低优空余")
FRAGMENTED = "碎卡"           # 有空卡但凑不满 / 高优+低优混合 —— 碎片来源
HI_WHOLE = "高优满占"         # 整节点被高优占满,不可回收


def node_low_priority_gpu(task_rows, low_pri_threshold: int = 3) -> dict:
    """{node_name: 低优卡数}。多节点任务平均摊(与 cmd_avail 同口径)。"""
    low = defaultdict(int)
    for r in task_rows:
        try:
            prio = int(r.get("优先级", 10))
        except (TypeError, ValueError):
            prio = 10
        if prio > low_pri_threshold:
            continue
        gpu = int(r.get("GPU", 0) or 0)
        nodes = [n for n in str(r.get("节点", "") or "").split(",") if n]
        if not nodes or gpu <= 0:
            continue
        per = gpu // len(nodes) if len(nodes) > 1 else gpu
        for n in nodes:
            low[n] += per
    return dict(low)


def _classify(used: int, total: int, low_pri: int) -> str:
    if total <= 0:
        return HI_WHOLE          # 异常节点(gpu_total=0),不计入可用统计
    if used <= 0:
        return EMPTY_WHOLE
    if low_pri >= total:
        return LOWPRI_WHOLE
    free = total - used
    if free == 0 and low_pri == 0:
        return HI_WHOLE
    return FRAGMENTED            # 有空卡 或 高低优混合 → 碎片来源


def compute_node_fragmentation(node_map, task_rows, low_pri_threshold: int = 3) -> dict:
    """返回 {"nodes": [...每节点...], "by_lcg": {lcg: {...聚合...}}}。"""
    low = node_low_priority_gpu(task_rows, low_pri_threshold)

    nodes = []
    for name, info in node_map.items():
        total = int(info.get("gpu_total", 0) or 0)
        used = int(info.get("gpu_used", 0) or 0)
        lp = min(int(low.get(name, 0) or 0), used)   # 摊出来的低优不会超过已用
        free = max(0, total - used)
        hp = max(0, used - lp)
        nodes.append({
            "node": name,
            "lcg": info.get("lcg", ""),
            "gpu_type": info.get("gpu_type", ""),
            "cluster": info.get("cluster", ""),
            "total": total, "used": used, "free": free,
            "low_pri": lp, "high_pri": hp,
            "class": _classify(used, total, lp),
        })

    by_lcg: dict = {}
    lcg_sizes = defaultdict(Counter)   # lcg -> Counter(gpu_total) 求众数当"节点卡数"
    for n in nodes:
        if n["total"] > 0:
            lcg_sizes[n["lcg"]][n["total"]] += 1

    for n in nodes:
        lcg = n["lcg"]
        agg = by_lcg.get(lcg)
        if agg is None:
            size = lcg_sizes[lcg].most_common(1)[0][0] if lcg_sizes[lcg] else 8
            agg = by_lcg[lcg] = {
                "lcg": lcg, "gpu_type": n["gpu_type"], "cluster": n["cluster"],
                "total_nodes": 0, "empty_whole": 0, "lowpri_whole": 0,
                "hi_whole": 0, "frag_nodes": 0,
                "frag_free_cards": 0, "frag_lowpri_cards": 0,
                "free_total": 0, "total_gpus": 0, "used_gpus": 0,
                "node_size": size,
            }
        if n["total"] <= 0:
            continue                     # 跳过异常节点
        agg["total_nodes"] += 1
        agg["total_gpus"] += n["total"]
        agg["used_gpus"] += n["used"]
        agg["free_total"] += n["free"]
        cls = n["class"]
        if cls == EMPTY_WHOLE:
            agg["empty_whole"] += 1
        elif cls == LOWPRI_WHOLE:
            agg["lowpri_whole"] += 1
        elif cls == HI_WHOLE:
            agg["hi_whole"] += 1
        else:  # FRAGMENTED
            agg["frag_nodes"] += 1
        # 碎片卡:散在"非整块"节点上的可回收卡
        if 0 < n["free"] < n["total"]:
            agg["frag_free_cards"] += n["free"]
        if 0 < n["low_pri"] < n["total"]:
            agg["frag_lowpri_cards"] += n["low_pri"]

    for agg in by_lcg.values():
        size = agg["node_size"] or 8
        reclaimable = agg["free_total"] + agg["frag_lowpri_cards"]
        agg["whole_node_potential"] = reclaimable // size
        tot = agg["total_gpus"]
        agg["util_pct"] = round(agg["used_gpus"] / tot * 100, 1) if tot else 0.0

    return {"nodes": nodes, "by_lcg": by_lcg}


# ---- 碎卡 → --exclude-node 参数桥（把"看到碎卡"接到"提交时避开"）----

def format_exclude_args(node_names) -> str:
    """把节点名列表拼成可直接粘贴的 `--exclude-node A --exclude-node B`。
    去空/strip/去重/排序,保证稳定可复现。"""
    names = sorted({str(n).strip() for n in node_names if n and str(n).strip()})
    return " ".join(f"--exclude-node {n}" for n in names)


def fragmented_node_names(frag_result, lcg=None, only_free=False) -> list:
    """从 compute_node_fragmentation 结果里挑碎卡节点名。

    lcg 限定某计算组;only_free=True 只挑还有空卡的碎卡节点(0<free<total)——
    这些正是新作业可能被"塞进去凑数"的节点,排掉它们逼调度器用整节点/空节点。
    """
    out = []
    for n in frag_result.get("nodes", []):
        if n.get("class") != FRAGMENTED:
            continue
        if lcg is not None and n.get("lcg") != lcg:
            continue
        if only_free and not (0 < n.get("free", 0) < n.get("total", 0)):
            continue
        out.append(n["node"])
    return out
