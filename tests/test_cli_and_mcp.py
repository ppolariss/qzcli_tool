import sys
import types

import qzcli.cli as cli


def _install_fake_mcp():
    fastmcp_module = types.ModuleType("mcp.server.fastmcp")

    class _FakeFastMCP:
        def __init__(self, *args, **kwargs):
            pass

        def tool(self, *args, **kwargs):
            def decorator(func):
                return func

            return decorator

        def run(self, *args, **kwargs):
            return None

    fastmcp_module.FastMCP = _FakeFastMCP

    mcp_module = types.ModuleType("mcp")
    server_module = types.ModuleType("mcp.server")
    server_module.fastmcp = fastmcp_module
    mcp_module.server = server_module

    sys.modules.setdefault("mcp", mcp_module)
    sys.modules.setdefault("mcp.server", server_module)
    sys.modules.setdefault("mcp.server.fastmcp", fastmcp_module)


_install_fake_mcp()

import qzcli.mcp_server as mcp_server


def test_mcp_resolve_workspace_refs_prefers_remote_for_all_workspaces(monkeypatch):
    remote_workspaces = [
        {"id": "ws-remote-1", "name": "远端空间一"},
        {"id": "ws-remote-2", "name": "远端空间二"},
    ]

    class _FakeAPI:
        def list_workspaces(self, cookie):
            assert cookie == "cookie=value"
            return remote_workspaces

    monkeypatch.setattr(mcp_server, "_require_cookie", lambda: ("cookie=value", {}))
    monkeypatch.setattr(
        mcp_server,
        "list_cached_workspaces",
        lambda: [{"id": "ws-cached", "name": "缓存空间"}],
    )
    monkeypatch.setattr(mcp_server, "get_api", lambda: _FakeAPI())

    assert mcp_server._resolve_workspace_refs(all_workspaces=True) == remote_workspaces


def test_workspace_parser_defaults_to_no_project_filter(monkeypatch):
    captured = {}

    def fake_cmd_workspace(args):
        captured["project"] = args.project
        captured["all"] = args.all
        return 0

    monkeypatch.setattr(cli, "cmd_workspace", fake_cmd_workspace)
    monkeypatch.setattr(sys, "argv", ["qzcli", "workspace"])

    assert cli.main() == 0
    assert captured == {"project": None, "all": False}
