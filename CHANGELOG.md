# Changelog

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
