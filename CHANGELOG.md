# Changelog

## v0.2.0 - 2026-05-04

First tagged release for qzcli. This release collects the recent CLI, API, and
MCP improvements on `master`.

### Added

- Added `qzcli logs <job-id>` backed by `/api/v2/train?Action=GetJobLog`, with
  cookie authentication, chronological tail output, and clearer login hints.
- Added SOCKS/HTTP proxy support for WSL/VPN environments via `proxy` in
  `~/.qzcli/config.json`, `QZCLI_PROXY`, or standard proxy environment variables.
- Added cookie-based job creation fallback for MCP job creation flows.
- Added optional credential resolution for `qz_auth_login` in the MCP server.
- Added interactive modeling/dev-machine listing and remote execution support.
- Added HPC/CPU job submission commands and documentation.
- Added HPC CPU/memory utilization and richer GPU availability output.
- Added progress display and faster code paths for `qzcli avail`.

### Changed

- Reworked internal POST calls to use a shared urllib3 layer so configured
  proxies are applied consistently across OpenAPI, cookie APIs, and v2 APIs.
- Improved workspace and compute group resolution, including compatibility for
  deprecated workspace flags.
- Improved login behavior for CAS/session-cookie based flows.
- Improved README coverage for login, GPU availability, logs, HPC, and create
  workflows.

### Fixed

- Fixed RSA password encryption so leading zero bytes are preserved.
- Fixed passwords containing special characters in login flows.
- Fixed workspace overview calls after the platform API endpoint changed.
- Fixed v2 logs response unwrapping and tail selection.
- Fixed targeted `avail` queries to avoid unnecessary workspace usage scans.
