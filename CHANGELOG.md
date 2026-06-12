# Changelog

## Unreleased

### What's new

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
