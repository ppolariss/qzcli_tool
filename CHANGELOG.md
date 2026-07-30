# Changelog

## Unreleased

## v0.4.0 - 2026-07-30

平台把 `/api/v1` 逐步下线，本版把 qzcli 的接口层迁到 `/api/v2`，并修掉多 agent 共用开发机时的一批并发问题。**没有 CLI 破坏性变更** —— 所有子命令的用法和输出形状保持不变。

### 平台接口 v1 → v2（v2 优先 + v1 兜底）

官方 CLI `qz` 已是纯 v2 客户端（11 services / 144 actions），而 qzcli 此前 16 个平台端点里只有 2 个在 v2。**v1 衰减不是假设**：`/openapi/v1/specs/list` 已经 404，qzcli 一直在静默回落本地缓存，而它正卡在 `qzcli create` 的关键路径上。

公开方法签名一律不变，内部拆成 `_xxx_v2` / `_xxx_v1` 两条腿，由 `api._v2_then_v1` 分发。已迁：`train ListJobs / GetJob / StopJob / ListJobEvents`、`notebook ListNotebooks`、`workspace ListNodeDimension / ListTaskDimension / GetBasicInfo / GetOverviewTaskMetric`、`hpc ListJobs`。

**回落判据是刻意收紧的**：只有路由不通（404/405/50x、被网关 302 成 HTML）才回落。业务错误（`AccessForbidden`、`InvalidParameter`）直接抛 —— 回落只会把权限问题和我们自己的请求 bug 一起藏起来。

三条只有真机实测才能发现的事：

- **`cluster.*` 对普通账号一律 `AccessForbidden`，必须走 `workspace.*` 双胞胎。** 两者在 `qz spec` 里的描述几乎一模一样，照字面映射会全线踩坑。
- **v2 没有任何 action 返回 `spec_id`**（144 个 action 的 schema 全 grep 过），`node_specs[]` 只有硬件参数没有 id。`list_specs` 改为从历史任务的 `framework_config[].instance_spec_price_info.quota_id` 反推 —— 这是目前唯一能从 v2 拿到真实 spec id 的路径。副作用是 `qzcli res -u` 把 specs 刷空后 `create` 仍然可用（此前会报「无法解析规格」）。
- **`inference-serving` 整个 service 在网关上 404**（spec 声明 18 个 action，路由没挂）。qzcli 不用它，不影响。

**两处仍在 v1**（v2 确实没有对应能力）：`/api/v1/project/list`（v2 的 `ListProjects` 普通账号无权限，且全域没有 `ListWorkspaces`）、`/api/v1/notebook/lab/{id}`（v2 拿不到 Jupyter 访问地址，`qzcli exec` 依赖它）。

### 多 agent 并发 exec：session 隔离

多个 AI agent 常同时对**同一台开发机**跑 `qzcli exec`，此前会互相串数据。修了三层：

- **job_id 撞车（主因）**：原来只有秒级时间戳，同一秒内起的多个 exec 拿到**完全相同的 job_id**，共用一个输出文件互相覆盖。3 路并发实测：第 3 路收到第 2 路的输出、第 1 路什么都没收到。现在 job_id 是 `qzcli_<session>_<ts>_<随机>`。
- **抢终端**：原来复用 `terms[0]`，不只 agent 之间互抢 —— 实测有开发机躺着 4 个别人两天前的人工交互终端，exec 会挑中第一个直接往里发命令。现在每次自建终端、用完删掉，命令用 `setsid`（无则 `nohup`）摘出去以免被带走。
- **文件永久泄漏**：清理只发生在拿到 exit 文件时，`--detach` 后没 attach 的、Ctrl-C 的、超时的全都一直留着。现在输出按 `/tmp/.qzcli/<session>/` 分目录，超过 7 天的旧 session 目录在下次 launch 时自动清理。

新增 `QZCLI_SESSION_ID`（env → `.env` → `config.json` → **按进程自动生成**，与凭据同一套优先级）。不设也不会串车。

新增 **`qzcli exec --list`**：列出开发机上的 exec 任务，默认只列本 session，`--all` 看全部。补上「`--detach` 之后忘了记 job_id 就再也找不回来」的缺口。

老格式 job_id（无 session 段）仍能 `exec-attach`，会回落到平铺路径。

### 修复

- **`qzcli hpc` 此前完全不可用**：平台新增必填 `priority`，不传直接被拒 `priority must be set`。已补 `--priority`。⚠️ **HPC 的优先级方向与训练任务相反** —— 实测提交 1→LOW(11)、3→LOW(13)、5→HIGH(30)、10→HIGH(35)，数字越大越高（有效 1-10），而训练任务的 `task_priority` 是 10 表示低优。默认取 1（LOW），与集群现有生产 HPC 任务一致。
- **`qzcli ws -w <中文名>` 直接崩**（`'latin-1' codec can't encode`）：`cmd_workspace` 拿 `-w` 的值当 workspace_id 用、从不解析名字，中文名原样拼进 referer 头导致请求发不出去。改为复用已有的 `_resolve_workspace_value`。
- **已禁用的工作空间会混进列表**：`project/list` 会把你不是成员的项目也返回，其 `space_list` 里可能挂着已禁用空间（v2 报 `AccessForbidden: 该空间已被禁用`，v1 反而返回陈旧集群结构）。`list_workspaces` 改为按 `usage_status != 0` 滤掉并提示跳过了哪些。
- **MCP 与 CLI 的提交路径分叉**：CLI 早已默认 v2 `CreateJobConsole`，`mcp_server` 还停在 v1，导致 v2 才支持的 `exclude_nodes` 在 MCP 侧静默失效。已统一。
- **`create_job_v2` 漏发 `x-inspire-client-source`**（缺它 APISIX 会把请求 302 到 Keycloak），且没挂 `@with_auth_retry`（提交中途 cookie 过期直接失败）。折进 `_request_v2` 后一并修好。
- 26 处硬编码 `https://qz.sii.edu.cn` 的 origin/referer 改用 `self.base_url` / `api.base_url`，非默认 `QZCLI_API_URL` 下不再发出错配的 Origin。

### 新增工具与文档（进仓，可复现）

| 文件 | 用途 |
|---|---|
| `tools/gen_api_spec_doc.py` | 扫 `qz spec`/`schema`/`--dry-run`，生成全部 144 个 action 的接口文档 |
| `tools/probe_v2.py` | cookie 可用性探针，只打只读 Action，产物自动对 UUID 打码 |
| `tools/compare_v1_v2.py` | v1/v2 逐字段 diff，防「静默返回空」 |
| `tools/live_smoke.py` | 活体冒烟，每个功能点在真实平台跑一遍（`--submit` 含真实提交+停止） |
| `docs/api_spec_v2.json` | 结构化接口定义，**平台改接口后 `git diff` 就能看出变了什么** |
| `docs/v1_to_v2_mapping.md` | 端点映射表（真机实测 + 踩坑 + 平台侧缺口） |
| `docs/v2_probe_report.md` | 109 个只读 Action 的 cookie 可用性结果 |

### 升级说明

- **无需改任何调用方式**，CLI 子命令和输出形状不变。
- `api.list_specs()` 多了一个可选参数 `workspace_id`（历史任务反推需要按工作空间查）。直接调用 `qzcli.api` 的代码不受影响；如果你 mock 或子类化了它，签名要跟着加。
- `QzAPIError` 新增 `api_code` 属性，承载 v2 信封里的 `ResponseMetadata.Error.Code`，用于按结构判断错误类型而不是抠错误文案。
- 多 agent 场景建议显式设 `QZCLI_SESSION_ID`；不设则每进程自动一个。

### 早前已在 Unreleased 中记录的改动

- **`qzcli dashboard` 成分下钻可视化看板 (P1)**: 新增子命令，用 Streamlit + plotly treemap/sunburst 把工作空间的在跑 GPU 占用按「**计算组(机房) → 优先级档 → 项目 → 用户 → 任务**」逐层下钻（块面积 = GPU 数，点块放大、面包屑退回），一眼看清「各计算组里谁占最多、各自高低优」；配色可切优先级/类型/**GPU 利用率**（暴露申请多却空转的任务）。关键在于计算组归属：`list_task_dimension` / `list_node_dimension` 都不直接带 logic_compute_group，改为逐 lcg 用 `list_node_dimension(logic_compute_group_id=…)` 反建「节点→计算组」映射（与 `avail` 同法），再经 `nodes_occupied` 挂到任务，实测 100% 覆盖。共享数据层 `fetch_all_task_dimensions` / `build_node_to_lcg_map` / `task_dimension_to_row` 落在 `cli.py`，`cmd_usage` 复用（输出不变）。下钻层级、视图（treemap/sunburst/icicle）、配色维度均可现场切换：配色支持 优先级/类型/**GPU 利用率**（红=申请多却空转）/**运行时长**（越久越红，按 95 分位截断避免超长任务拉平色阶）。工作空间用**下拉框**选（读本地 `resources.json` 缓存）；顶部有**按任务类型占比**行（交互式建模/训练/推理各占多少 GPU），以及**已占用/空闲 GPU** KPI；勾选「叠加空闲 GPU（灰块）」把各计算组的**剩余容量**（节点 `gpu.total − used` 聚合）以灰块叠加。悬停任意块给出干净明细卡片（任务数/类型/GPU 加权平均利用率/最长运行时长），对内部节点也做了逐层聚合（不再是 px 默认的 `NaN`/`(?)`）。任务分页与逐 lcg 节点查询用 `ThreadPoolExecutor` **并发拉取**（分布式空间首屏 ~17s → ~5s），`cmd_usage` 同样受益。看板依赖走可选 extra：`pip install 'qzcli[dashboard]'`。
- **Cookie 过期自动重登 (P0)**: 所有 cookie 认证的 API 方法（`*_with_cookie`、`_request_v2`、HPC/节点维度查询等）现在用 `@with_auth_retry` 装饰——遇到 401 会用本地凭据透明地 `login_with_cas` 重登一次并重试，消除了此前在长会话 / 自动化中每隔 ~20 分钟手动 `qzcli login` 的反复操作。无凭据或重登失败时回退到原有行为（如 token 认证）。`exec` 取 Jupyter 连接信息时同样会在 cookie 过期时自动重登。
- **CAS 登录重试退避 (P1)**: `login_with_cas` 现在对瞬时故障（SSL `UNEXPECTED_EOF_WHILE_READING`、连接重置、CAS/代理 5xx）做指数退避重试（最多 3 次）；用户名密码错误等永久性错误立即抛出、不重试。新增 `QzTransientError`（`QzAPIError` 子类）用类型而非文案标记可重试错误。
- **`exec` 分离式后台执行 (P1)**: `qzcli exec --detach`（别名 `--no-wait`）后台启动命令并立即返回 `job_id`；`qzcli exec-attach <target> <job_id>` 重连并继续拉取输出。`exec` 超时不再丢弃输出，而是保留远端文件并打印可直接复制的 `exec-attach` 续读命令。底层 `_exec_via_jupyter` 拆分为 `_exec_launch` / `_exec_poll`。
- **新增 MCP 工具 `qz_exec` / `qz_exec_attach`**: agent 无需 shell-out 即可在开发机执行命令；`detach=True` 用于编译、下载、训练等长命令，配合 `qz_exec_attach` 轮询结果。两者共享上面的 cookie 自动重登。
- **`exec` / `exec-attach` 的 target 支持 notebook_id 及其前缀**: 除了名字、完整 UUID、URL，现在也能直接粘贴 notebook_id 或它的一段前缀；`_resolve_notebook_id_by_name` 先按 name/notebook_id 精确命中，未命中再按 notebook_id 前缀模糊匹配——前缀唯一才解析，撞到多个时列出候选并报错（不默默取第一个）。CLI 与 MCP `qz_exec` 共用此解析路径。

## v0.3.0 - 2026-05-28

Second tagged release. Two breaking changes around resource refresh and the job-creation payload, plus exec polish and several reliability fixes since v0.2.0.

### Highlights

- **`qzcli exec` now production-ready for pasted targets**: accepts dev-machine name, notebook UUID, **or** a full IDE / Jupyter URL pasted from the browser, with new `--timeout` flag and a user_ids-filter fix that previously hid other people's dev machines.
- **Much faster `qzcli res -u`**: default-flip to quick mode + parallel workspace refresh drops a full cache refresh on 19 workspaces from "hangs indefinitely" to under a minute.
- **`qzcli create` migrated to the new `resource_spec_price` schema**: the platform rejected the legacy `framework_config[0].spec_id` field with `unknown field "spec_id"`; new payload also promotes cpu / mem_gi / gpu_count to satisfy the platform's `Cpu and Mem can't be empty.` check.

### Breaking changes

- `qzcli res -u` now defaults to **quick mode** — skips the unbounded historical-jobs walk and pulls `compute_groups` / `projects` directly from `get_cluster_basic_info` / `list_task_dimension`. `specs` are no longer refreshed in the default path. Pass `--full` / `-F` to opt back into the legacy full scan (still required when you need fresh spec ids for `cmd_create` non-interactive). The `--quick` / `-q` flag is preserved as a no-op for backward compatibility with existing scripts. `create -i` still uses the full scan internally because submission needs spec ids.
- `qzcli create` and the `qz_create_job` MCP tool now build the `/api/v1/train_job/create` payload using the platform's new `resource_spec_price` schema; the legacy `framework_config[0].spec_id` field is no longer sent. If you were assembling payloads by hand against an older platform build, regenerate them.

### What's new

- `qzcli exec`: target argument now accepts dev-machine name, notebook UUID, **or** a full IDE / Jupyter URL pasted from the browser (`/ide?notebook_id=...`, `/jobs/interactiveModel(ing)?Detail/...`, `/jupyter/...`, `/api/v1/notebook/lab/...`, `/notebook/(lab|code)/...`); add `--timeout` flag (default 120s, must precede `target` because `remote_cmd` uses `argparse.REMAINDER`); drop the `user_ids` filter on the underlying `list_notebooks` call so name resolution no longer hides dev machines whose `created_by` differs from the caller. See the new "远程执行 / 开发机命令" section in README for usage. (aad08d0, #29, #30)
- `qzcli res -u` now refreshes workspaces **in parallel** via `ThreadPoolExecutor`. Default `--parallel 8`; pass `--parallel 1` to recover the old sequential behavior (useful when debugging or when a workspace's API is misbehaving). Disk writes (`save_resources`) still run on the main thread as results land, so there's no read-modify-write race on `~/.qzcli/resources.json`. Combined with the default-quick flip, full `res -u` on 19 workspaces drops from "hangs indefinitely" to under a minute end-to-end on this machine.
- `qzcli res -u` now shows a live progress bar during workspace cache refresh, sharing the rich `display.create_progress()` pattern from `qzcli avail`.
- Reapply most of PR #23 (cookie auth for `get_job_detail` / `stop_job`, `qz_get_hpc_usage` unpacking fix, `qz_track_job` error propagation). The `_get_token` encrypt-password change is intentionally *not* reapplied — the `{"encrypted": True}` flag is not part of `/auth/token`'s documented contract and likely doesn't help CAS-federated users with `invalid_grant` anyway. Issue #14 remains open.
- `qzcli create` now prefers cookie auth (`/api/v1/train_job/create`) by default and only falls back to the openapi token path when no cookie is available — this aligns the CLI with `qz_create_job` and unblocks CAS-federated users who previously got `invalid_grant`.
- New public helper `qzcli.api.build_resource_spec_price(spec_obj, compute_group_id)` shared by CLI and MCP. New CLI helper `_lookup_spec_for_payload` auto-refreshes the spec cache when cpu/gpu/memory fields are missing, and gives a `qzcli res -u` hint if still unresolved.
- Documentation: README now documents `qzcli exec` in the 任务管理 table and a new 远程执行 / 开发机命令 subsection (was completely missing despite shipping in v0.2.0). (#30)

### Upgrade notes

- No Python version change (`>=3.8`).
- If you rely on `qzcli res -u` populating `specs`, add `--full` to your refresh script.
- If you assemble `/api/v1/train_job/create` payloads yourself, switch to the new `resource_spec_price` schema; `framework_config[0].spec_id` is now rejected by the platform.

### Validation

- `python3 -m compileall qzcli tests`
- `python3 -m unittest discover -s tests`

## v0.2.0 - 2026-05-04

qzcli v0.2.0 is the first tagged release for the project, collecting the recent work on job logs, WSL/VPN networking, faster capacity checks, MCP integration, interactive workloads, and HPC/CPU job submission into a versioned release.

## Highlights

- WSL/VPN Proxy Support: Add config-driven SOCKS and HTTP(S) proxy routing across OpenAPI calls, cookie-authenticated APIs, `/api/v2` calls, and CAS login, including explicit handling for WSL/Clash setups where `HTTPS_PROXY=http://...` can override a SOCKS proxy, plus a declared `PySocks` dependency for reliable installation: #13
- Fast Availability Queries: Preserve cached fuzzy workspace matching, avoid slow full workspace scans for targeted `qzcli avail -w` queries, parallelize node/task dimension fetches, increase low-priority task pagination, add progress display, and reduce measured local `qzcli avail` runtime from about 49.7s to about 5.9s in the optimized path: #17
- Job Logs CLI: Add `qzcli logs <job-id>` backed by `/api/v2/train?Action=GetJobLog` with cookie authentication, chronological tail output, `--tail`, `--follow`, `--raw`, `--json`, `--pod`, and `--since` support, plus documentation and clearer login failure hints: #16, 0e66dc4, 0d6c706, 16fed90
- Job Creation and MCP Workflows: Add `qzcli create`, `qzcli batch`, MCP job creation tools, cookie-based job creation fallback, optional MCP login credential resolution, and interactive job submission with auto-login support: #7, #12, #13
- Interactive and HPC Workloads: Add dev-machine listing, Jupyter-terminal based remote command execution, HPC/CPU job submission, HPC command documentation, and HPC CPU/memory utilization in availability output: #9, #10, 93e40a6, fa8cc23
- Login and Workspace Reliability: Fix CAS RSA ciphertext encoding when leading zero bytes are present, handle passwords with special characters, move workspace overview to the new platform endpoint, and keep backward-compatible workspace flags as deprecated no-ops: #8, #11, 4cf71ce, ed10a5f

## Upgrade Notes

- No known breaking changes; Python support remains `>=3.8`.
- SOCKS proxy users should install updated dependencies with `pip install -r requirements.txt`.
- WSL users can configure a Windows-side Clash proxy with `{"proxy": "socks5://127.0.0.1:7897"}` in `~/.qzcli/config.json`.
- `qzcli logs` and other `/api/v2` calls require cookie authentication; run `qzcli login` again if a command reports an expired or missing cookie.

## Dependencies

- Add `PySocks>=1.7` so SOCKS proxy support works after a normal requirements install: #13

## Validation

- `python3 -m compileall qzcli tests`
- `python3 -m unittest discover -s tests` (54 passed, 1 skipped)

## What's Changed

- Add sii-cas-auth auto login password encryption: #1
- Unify `avail` table output and improve formatting: #2
- Add qzcli MCP server support: #6
- Add `qzcli create` and `qzcli batch` commands for job submission: #7
- Preserve leading zero in RSA ciphertext: #8
- Add dev machine listing and Jupyter-based remote exec: #9
- Add HPC/CPU job submission support: #10
- Update workspace overview to use the new API endpoint: #11
- Support interactive job submitting and auto-login: #12
- Support SOCKS5 proxy for WSL/VPN environments: #13
- Document `qzcli logs`: #16
- Preserve cached workspace matches and speed up `qzcli avail`: #17

Full Changelog: https://github.com/tianyilt/qzcli_tool/commits/v0.2.0
