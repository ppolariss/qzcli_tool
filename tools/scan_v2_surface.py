#!/usr/bin/env python3
"""扫出平台 v2 的**真实**接口面，并和 qz CLI 的 spec 对账。

## 为什么要有这个

我们踩过一次实打实的坑：把 `docs/api_spec_v2.json`（qz CLI 的 spec）当成了 v2 的
完整接口面，于是得出两个错误结论并发布出去 ——

- 「v2 拿不到点券/预算数据」→ 实际 `project GetProjectBudgetUsageOverview` 一直在
- 「平台无 `ListWorkspaces`」→ 实际 `workspace ListWorkspaces` 一直在

实测 spec 只收了 11 个服务，而 Web 控制台在用 21 个；`project` 服务 spec 里 1 个
action，前端在用 32 个。**spec 是子集，不是全集。**

所以判断「v2 有没有某能力」必须实测，不能查 spec。这个工具把实测流程固化下来。

## 它做什么

1. 抓 Web 控制台的前端产物，正则提取写死的 `/api/v2/{service}?Action={Action}`
2. 和 spec 对账，列出「spec 没有但平台有」的部分
3. 对**只读** action 逐个发一次最小请求，按服务端返回分类：

   =================  ==========================================
   分类               判据
   =================  ==========================================
   ``可用``           无 Error，返回了业务数据
   ``需参数``         ``InvalidParameter`` —— 路由在，参数不对
   ``权限``           ``AccessForbidden`` —— 路由在，当前账号没权限
   ``不存在``         ``InvalidAction: unknown action``
   =================  ==========================================

   注意后三类都证明**路由存在**，只有「不存在」是真没有。

## 安全边界

**只探只读 action。** 写操作（Create/Delete/Update/Stop/Apply/…）一律跳过 ——
这个工具会拿真实登录态打生产环境，误触一个 `DeleteProjectQuota` 不是小事。
白名单是「动词前缀必须是 Get/List/Check/Describe/Query/Search」，
再叠一层黑名单兜底。宁可漏扫，不可误触。

## 用法

    python3 tools/scan_v2_surface.py                    # 全量：抓 JS + 对账 + 探活
    python3 tools/scan_v2_surface.py --no-probe         # 只抓 JS 和对账，不发请求
    python3 tools/scan_v2_surface.py --service audit    # 只看某个服务
    python3 tools/scan_v2_surface.py --delay 1.0        # 放慢，避免撞限流

全量探活会发几百个请求。默认每个之间隔 ``--delay`` 秒（默认 0.4），
别和 `parity_sweep` / `live_smoke` 背靠背跑，会把配额打满。
"""

import argparse
import collections
import hashlib
import json
import pathlib
import re
import sys
import time

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import requests  # noqa: E402

from qzcli.api import get_api  # noqa: E402
from qzcli.config import get_cookie  # noqa: E402

BASE = "https://qz.sii.edu.cn"

#: 只探这些动词开头的 action。宁可漏扫，不可误触生产数据。
READ_PREFIXES = ("Get", "List", "Check", "Describe", "Query", "Search")

#: 兜底黑名单 —— 万一有个叫 `GetOrCreateXxx` 的混进来。
WRITE_WORDS = re.compile(
    r"Create|Delete|Update|Remove|Add|Stop|Start|Apply|Cancel|Submit|Operate"
    r"|Scale|Rollback|Preheat|Save|Bind|Transfer|Publish|Import|Restart|Reset"
    r"|Set|Modify|Move|Rename|Upload|Kill|Terminate|Approve|Reject"
)


def is_read_only(action: str) -> bool:
    return action.startswith(READ_PREFIXES) and not WRITE_WORDS.search(action)


def fetch_frontend_actions(cookie, cache_dir, verbose=False):
    """抓前端产物，提取写死的 ``/api/v2/{service}?Action={Action}``。

    返回值是**下界** —— 动态拼接的调用抓不到。所以这个工具只用来证明
    「某 action 存在」，不能用来证明「某 action 不存在」。
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    s = requests.Session()
    for kv in (cookie or "").split(";"):
        if "=" in kv:
            k, v = kv.strip().split("=", 1)
            s.cookies.set(k, v, domain="qz.sii.edu.cn")

    r = s.get(BASE + "/", timeout=60)
    entries = re.findall(r'src="([^"]+\.js)"', r.text)
    seen, todo, n = set(), list(entries), 0
    while todo:
        u = todo.pop(0)
        if u in seen:
            continue
        seen.add(u)
        cached = cache_dir / (hashlib.md5(u.encode()).hexdigest() + ".js")
        if cached.exists():
            text = cached.read_text(encoding="utf-8", errors="replace")
        else:
            try:
                resp = s.get(BASE + u, timeout=90)
            except requests.RequestException:
                continue
            if resp.status_code != 200:
                continue
            text = resp.text
            cached.write_text(text, encoding="utf-8")
        n += 1
        for m in set(re.findall(r'"(\./[^"]+\.js)"', text)):
            p = "/assets/" + m[2:]
            if p not in seen:
                todo.append(p)
    if verbose:
        print(f"[抓取] {n} 个 chunk", file=sys.stderr)

    found = collections.defaultdict(set)
    for f in cache_dir.glob("*.js"):
        t = f.read_text(encoding="utf-8", errors="replace")
        # action 名可能带数字（实测 GetProjectListV2）。用 [A-Za-z]+ 会把它截成
        # GetProjectListV，然后探活报「不存在」—— 一个自己造出来的假缺口。
        for svc, act in re.findall(r"/api/v2/([a-z\-]+)\?Action=([A-Za-z0-9_]+)", t):
            found[svc].add(act)
    return found


def load_spec(path):
    d = json.loads(pathlib.Path(path).read_text(encoding="utf-8"))
    return {k: set(v.get("action_details", {})) for k, v in d["services"].items()}


def classify(api, cookie, svc, act, delay):
    """发一次最小请求，按返回分类。空 body 就够 —— 目的是问路由在不在。"""
    time.sleep(delay)
    try:
        r = api._request_v2(
            svc, act, {}, cookie=cookie, referer_path="/operations/projects", raw=True
        )
    except Exception as exc:  # noqa: BLE001 —— 分类器要吃下所有情况并归类
        return "异常", f"{type(exc).__name__}: {exc}"[:120]
    err = ((r or {}).get("ResponseMetadata") or {}).get("Error") or {}
    code, msg = err.get("Code"), str(err.get("Message") or "")
    if not code:
        keys = [k for k in (r or {}) if k != "ResponseMetadata"]
        return "可用", f"返回 {keys}"
    if code == "InvalidAction" and "unknown action" in msg:
        return "不存在", msg[:100]
    if code == "AccessForbidden":
        return "权限", msg[:100]
    if code == "InvalidParameter":
        return "需参数", msg[:100]
    return code, msg[:100]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--spec", default="docs/api_spec_v2.json")
    ap.add_argument("--out", default="docs/v2_surface_scan.md")
    ap.add_argument("--cache", default=".cache/qz_frontend_js")
    ap.add_argument("--service", help="只处理某个服务")
    ap.add_argument("--delay", type=float, default=0.4)
    ap.add_argument("--no-probe", action="store_true", help="只对账，不发请求")
    args = ap.parse_args()

    cookie = (get_cookie() or {}).get("cookie")
    if not cookie:
        print("没有登录态，先 qzcli login", file=sys.stderr)
        return 2
    api = get_api()

    fe = fetch_frontend_actions(cookie, pathlib.Path(args.cache), verbose=True)
    sp = load_spec(args.spec)
    services = sorted(set(fe) | set(sp))
    if args.service:
        services = [s for s in services if s == args.service]

    lines = [
        "# 平台 v2 真实接口面扫描",
        "",
        "由 `tools/scan_v2_surface.py` 生成。**前端列是下界**（只统计写死的调用），",
        "所以本表能证明「某 action 存在」，不能证明「某 action 不存在」。",
        "",
        "## 服务级对账",
        "",
        "| 服务 | spec | 前端 | spec 缺 |",
        "|---|---:|---:|---|",
    ]
    for s in services:
        a, b = sp.get(s, set()), fe.get(s, set())
        miss = sorted(b - a)
        lines.append(
            f"| `{s}` | {len(a)} | {len(b)} | "
            f"{('**' + str(len(miss)) + '**') if miss else '—'} |"
        )
    lines += [
        "",
        f"合计：spec {sum(len(v) for v in sp.values())} 个 action / {len(sp)} 服务；"
        f"前端 {sum(len(v) for v in fe.values())} 个 / {len(fe)} 服务。",
        "",
    ]

    if not args.no_probe:
        lines += [
            "## 只读 action 探活",
            "",
            "写操作一律跳过（见模块 docstring 的安全边界）。",
            "`需参数` / `权限` **都证明路由存在**，只有 `不存在` 是真没有。",
            "",
            "| 服务 | Action | 结果 | 详情 |",
            "|---|---|---|---|",
        ]
        stats = collections.Counter()
        for s in services:
            for act in sorted(fe.get(s, set()) | sp.get(s, set())):
                if not is_read_only(act):
                    stats["跳过(写操作)"] += 1
                    continue
                kind, detail = classify(api, cookie, s, act, args.delay)
                stats[kind] += 1
                lines.append(
                    f"| `{s}` | `{act}` | {kind} | {detail.replace('|', '/')} |"
                )
                print(f"  {s:20} {act:38} {kind}", flush=True)
        lines += ["", "### 汇总", ""]
        for k, v in stats.most_common():
            lines.append(f"- {k}: {v}")

    out = pathlib.Path(args.out)
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\n✓ 写入 {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
