# Changelog

## v0.2.0 - 2026-05-04

qzcli v0.2.0 is the first tagged release. It packages the recent work around
job logs, WSL/VPN proxy support, faster availability checks, MCP usability, and
job submission workflows into a release that can be referenced and rolled back
cleanly.

### Highlights

- **Job logs from the CLI:** `qzcli logs <job-id>` now reads platform logs
  through `/api/v2/train?Action=GetJobLog`, using the same cookie session as
  `qzcli login`. ([#16](https://github.com/tianyilt/qzcli_tool/pull/16))
- **WSL/VPN proxy support:** network calls can now use SOCKS or HTTP(S) proxy
  settings from `~/.qzcli/config.json`, `QZCLI_PROXY`, or standard proxy
  environment variables. ([#13](https://github.com/tianyilt/qzcli_tool/pull/13))
- **Faster `qzcli avail`:** availability queries now preserve cached workspace
  matches, avoid unnecessary scans for targeted queries, and show progress
  during longer capacity checks. ([#17](https://github.com/tianyilt/qzcli_tool/pull/17))
- **Broader job workflows:** interactive modeling, HPC/CPU submission, resource
  usage, and MCP job creation are now covered by the CLI/MCP surfaces.
  ([#6](https://github.com/tianyilt/qzcli_tool/pull/6),
  [#7](https://github.com/tianyilt/qzcli_tool/pull/7),
  [#9](https://github.com/tianyilt/qzcli_tool/pull/9),
  [#10](https://github.com/tianyilt/qzcli_tool/pull/10),
  [#12](https://github.com/tianyilt/qzcli_tool/pull/12))

### Breaking Changes

- No known breaking changes.
- Python support remains `>=3.8`.

### Upgrade Notes

- SOCKS proxy users should install the updated dependencies so `PySocks` is
  available:

  ```bash
  pip install -r requirements.txt
  ```

- To use a Windows-side Clash proxy from WSL, add this to
  `~/.qzcli/config.json`:

  ```json
  {
    "proxy": "socks5://127.0.0.1:7897"
  }
  ```

- `qzcli logs` and other `/api/v2` calls require cookie auth. Run `qzcli login`
  again if the command reports an expired or missing cookie.

### New Features

- Added `qzcli logs <job-id>` with chronological tail output and clearer login
  guidance. ([#16](https://github.com/tianyilt/qzcli_tool/pull/16))
- Added SOCKS/HTTP proxy handling across OpenAPI, cookie API, and v2 API calls.
  ([#13](https://github.com/tianyilt/qzcli_tool/pull/13))
- Added cookie-based job creation fallback for MCP job creation.
  ([#13](https://github.com/tianyilt/qzcli_tool/pull/13))
- Added optional credential resolution for `qz_auth_login` in the MCP server.
  ([#13](https://github.com/tianyilt/qzcli_tool/pull/13))
- Added interactive modeling/dev-machine listing and remote execution support.
  ([#9](https://github.com/tianyilt/qzcli_tool/pull/9))
- Added interactive job submission and auto-login improvements.
  ([#12](https://github.com/tianyilt/qzcli_tool/pull/12))
- Added `qzcli create`, `qzcli batch`, and MCP job creation tools.
  ([#7](https://github.com/tianyilt/qzcli_tool/pull/7))
- Added HPC/CPU job submission commands and README documentation.
  ([#10](https://github.com/tianyilt/qzcli_tool/pull/10))
- Added HPC CPU/memory utilization and richer GPU availability output.
  ([93e40a6](https://github.com/tianyilt/qzcli_tool/commit/93e40a6),
  [fa8cc23](https://github.com/tianyilt/qzcli_tool/commit/fa8cc23))

### Improvements

- Reworked POST calls onto a shared urllib3 layer so proxy behavior is
  consistent and connection pooling is preserved.
  ([#13](https://github.com/tianyilt/qzcli_tool/pull/13))
- Improved workspace and compute group resolution, including compatibility for
  deprecated workspace flags.
  ([#11](https://github.com/tianyilt/qzcli_tool/pull/11),
  [#17](https://github.com/tianyilt/qzcli_tool/pull/17))
- Improved CAS/session-cookie login behavior.
  ([#1](https://github.com/tianyilt/qzcli_tool/pull/1),
  [#12](https://github.com/tianyilt/qzcli_tool/pull/12),
  [#13](https://github.com/tianyilt/qzcli_tool/pull/13))
- Improved README coverage for login, GPU availability, logs, HPC, and create
  workflows.
  ([#7](https://github.com/tianyilt/qzcli_tool/pull/7),
  [#10](https://github.com/tianyilt/qzcli_tool/pull/10),
  [#16](https://github.com/tianyilt/qzcli_tool/pull/16))

### Bug Fixes

- Fixed RSA password encryption so leading zero bytes are preserved.
  ([#8](https://github.com/tianyilt/qzcli_tool/pull/8))
- Fixed passwords containing special characters in login flows.
  ([4cf71ce](https://github.com/tianyilt/qzcli_tool/commit/4cf71ce))
- Fixed workspace overview calls after the platform API endpoint changed.
  ([#11](https://github.com/tianyilt/qzcli_tool/pull/11))
- Fixed v2 logs response unwrapping and tail selection.
  ([0d6c706](https://github.com/tianyilt/qzcli_tool/commit/0d6c706),
  [16fed90](https://github.com/tianyilt/qzcli_tool/commit/16fed90))
- Fixed targeted `avail` queries to avoid unnecessary workspace usage scans.
  ([#17](https://github.com/tianyilt/qzcli_tool/pull/17))

### Dependencies

- Added `PySocks>=1.7` for SOCKS proxy support.
  ([#13](https://github.com/tianyilt/qzcli_tool/pull/13))

### Validation

- `python3 -m compileall qzcli tests`
- `python3 -m unittest discover -s tests` (`54 passed, 1 skipped`)

### What's Changed

- Add sii-cas-auth auto login password encryption
  ([#1](https://github.com/tianyilt/qzcli_tool/pull/1))
- Unify `avail` table output and improve formatting
  ([#2](https://github.com/tianyilt/qzcli_tool/pull/2))
- Add qzcli MCP server support
  ([#6](https://github.com/tianyilt/qzcli_tool/pull/6))
- Add `qzcli create` and `qzcli batch` commands for job submission
  ([#7](https://github.com/tianyilt/qzcli_tool/pull/7))
- Preserve leading zero in RSA ciphertext
  ([#8](https://github.com/tianyilt/qzcli_tool/pull/8))
- Add dev machine listing and Jupyter-based remote exec
  ([#9](https://github.com/tianyilt/qzcli_tool/pull/9))
- Add HPC/CPU job submission support
  ([#10](https://github.com/tianyilt/qzcli_tool/pull/10))
- Update workspace overview to use the new API endpoint
  ([#11](https://github.com/tianyilt/qzcli_tool/pull/11))
- Support interactive job submitting and auto-login
  ([#12](https://github.com/tianyilt/qzcli_tool/pull/12))
- Support SOCKS5 proxy for WSL/VPN environments
  ([#13](https://github.com/tianyilt/qzcli_tool/pull/13))
- Document `qzcli logs`
  ([#16](https://github.com/tianyilt/qzcli_tool/pull/16))
- Preserve cached workspace matches and speed up `qzcli avail`
  ([#17](https://github.com/tianyilt/qzcli_tool/pull/17))

### Full Changelog

- <https://github.com/tianyilt/qzcli_tool/commits/v0.2.0>
