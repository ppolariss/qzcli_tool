import sys
import socket
import types
from argparse import Namespace
from http.server import BaseHTTPRequestHandler

import qzcli.cli as cli
import qzcli.create_commands as create_commands
import qzcli.resource_commands as resource_commands
import qzcli.task_dimensions as task_dimensions


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


def test_tasks_parser_defaults_to_served_dashboard(monkeypatch):
    captured = {}

    def fake_cmd_task_dimensions(args):
        captured["workspace"] = args.workspace
        captured["project"] = args.project
        captured["page_size"] = args.page_size
        captured["serve"] = args.serve
        captured["command"] = args.command
        return 0

    monkeypatch.setattr(cli, "cmd_task_dimensions", fake_cmd_task_dimensions)
    monkeypatch.setattr(sys, "argv", ["qzcli", "tasks"])

    assert cli.main() == 0
    assert captured == {
        "workspace": None,
        "project": None,
        "page_size": 100,
        "serve": True,
        "command": "tasks",
    }


def test_blame_alias_can_disable_dashboard(monkeypatch):
    captured = {}

    def fake_cmd_task_dimensions(args):
        captured["serve"] = args.serve
        captured["command"] = args.command
        return 0

    monkeypatch.setattr(cli, "cmd_task_dimensions", fake_cmd_task_dimensions)
    monkeypatch.setattr(sys, "argv", ["qzcli", "blame", "--no-serve"])

    assert cli.main() == 0
    assert captured == {"serve": False, "command": "blame"}


def test_task_dashboard_skips_busy_port():
    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):  # noqa: A003
            return None

    busy = socket.socket()
    server = None
    try:
        busy.bind(("127.0.0.1", 0))
        busy.listen(1)
        busy_port = busy.getsockname()[1]

        server, requested = task_dimensions._bind_dashboard_server(
            "127.0.0.1", busy_port, _Handler
        )

        assert requested == busy_port
        assert server.server_address[1] != busy_port
        assert busy_port < server.server_address[1] < busy_port + 50
    finally:
        if server is not None:
            server.server_close()
        busy.close()


def test_hpc_jobs_parser_accepts_created_by_and_status(monkeypatch):
    captured = {}

    def fake_cmd_hpc_jobs(args):
        captured["workspace"] = args.workspace
        captured["created_by"] = args.created_by
        captured["mine"] = args.mine
        captured["status"] = args.status
        captured["queued"] = args.queued
        captured["limit"] = args.limit
        captured["command"] = args.command
        return 0

    monkeypatch.setattr(cli, "cmd_hpc_jobs", fake_cmd_hpc_jobs)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "qzcli",
            "hpc-jobs",
            "-w",
            "CPU资源空间",
            "--created-by",
            "user-1",
            "-s",
            "QUEUEING",
            "-n",
            "50",
        ],
    )

    assert cli.main() == 0
    assert captured == {
        "workspace": "CPU资源空间",
        "created_by": "user-1",
        "mine": False,
        "status": "QUEUEING",
        "queued": False,
        "limit": 50,
        "command": "hpc-jobs",
    }


def test_hpc_jobs_parser_accepts_mine(monkeypatch):
    captured = {}

    def fake_cmd_hpc_jobs(args):
        captured["workspace"] = args.workspace
        captured["created_by"] = args.created_by
        captured["mine"] = args.mine
        captured["status"] = args.status
        return 0

    monkeypatch.setattr(cli, "cmd_hpc_jobs", fake_cmd_hpc_jobs)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "qzcli",
            "hpc-jobs",
            "-w",
            "CPU资源空间",
            "--mine",
            "-s",
            "RUNNING",
        ],
    )

    assert cli.main() == 0
    assert captured == {
        "workspace": "CPU资源空间",
        "created_by": None,
        "mine": True,
        "status": "RUNNING",
    }


def test_user_jobs_parser_accepts_created_by_and_filters(monkeypatch):
    captured = {}

    def fake_cmd_user_jobs(args):
        captured["created_by"] = args.created_by
        captured["workspace"] = args.workspace
        captured["kind"] = args.kind
        captured["queued"] = args.queued
        captured["limit"] = args.limit
        captured["command"] = args.command
        return 0

    monkeypatch.setattr(cli, "cmd_user_jobs", fake_cmd_user_jobs)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "qzcli",
            "user-jobs",
            "--created-by",
            "user-1",
            "--workspace",
            "CPU资源空间",
            "--kind",
            "all",
            "--queued",
            "-n",
            "100",
        ],
    )

    assert cli.main() == 0
    assert captured == {
        "created_by": "user-1",
        "workspace": "CPU资源空间",
        "kind": "all",
        "queued": True,
        "limit": 100,
        "command": "user-jobs",
    }


def test_user_jobs_parser_allows_default_current_user(monkeypatch):
    captured = {}

    def fake_cmd_user_jobs(args):
        captured["created_by"] = args.created_by
        captured["kind"] = args.kind
        captured["running"] = args.running
        return 0

    monkeypatch.setattr(cli, "cmd_user_jobs", fake_cmd_user_jobs)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "qzcli",
            "user-jobs",
            "--kind",
            "hpc",
            "--running",
        ],
    )

    assert cli.main() == 0
    assert captured == {
        "created_by": None,
        "kind": "hpc",
        "running": True,
    }


def test_resolve_created_by_uses_cached_current_user(monkeypatch):
    monkeypatch.setattr(
        cli,
        "load_config",
        lambda: {"username": "login-1", "user_id": "user-1"},
    )

    class _UnexpectedAPI:
        def list_hpc_jobs_with_cookie(self, *args, **kwargs):
            raise AssertionError("cache should avoid API probing")

    assert (
        cli._resolve_created_by_selector(
            _UnexpectedAPI(),
            "cookie=value",
            [{"id": "ws-1", "name": "CPU资源空间"}],
            "login-1",
            include_train=False,
            include_hpc=True,
        )
        == "user-1"
    )
    assert (
        cli._resolve_created_by_selector(
            _UnexpectedAPI(),
            "cookie=value",
            [{"id": "ws-1", "name": "CPU资源空间"}],
            "me",
            include_train=False,
            include_hpc=True,
        )
        == "user-1"
    )


def test_resolve_created_by_probes_hpc_and_caches_current_user(monkeypatch):
    saved = {}
    config = {"username": "login-1"}
    monkeypatch.setattr(cli, "load_config", lambda: dict(config))
    monkeypatch.setattr(cli, "save_config", lambda data: saved.update(data))

    class _FakeAPI:
        def list_hpc_jobs_with_cookie(self, workspace_id, cookie, page_size=100):
            assert workspace_id == "ws-1"
            assert cookie == "cookie=value"
            assert page_size == 100
            return {
                "jobs": [
                    {
                        "created_by": {
                            "id": "user-1",
                            "name": "张三",
                            "name_en": "zhangsan",
                            "extra_info": {
                                "institution_id": "login-1",
                                "login_name": "login-1",
                            },
                        }
                    }
                ]
            }

    resolved = cli._resolve_created_by_selector(
        _FakeAPI(),
        "cookie=value",
        [{"id": "ws-1", "name": "CPU资源空间"}],
        "login-1",
        include_train=False,
        include_hpc=True,
    )

    assert resolved == "user-1"
    assert saved["user_id"] == "user-1"
    assert saved["login_name"] == "login-1"


def test_mcp_list_hpc_jobs_uses_hpc_api(monkeypatch):
    calls = []

    class _FakeAPI:
        def list_hpc_jobs_with_cookie(
            self,
            workspace_id,
            cookie,
            page_size=100,
            created_by=None,
            status=None,
            **kwargs,
        ):
            calls.append(
                {
                    "workspace_id": workspace_id,
                    "cookie": cookie,
                    "page_size": page_size,
                    "created_by": created_by,
                    "status": status,
                }
            )
            return {
                "total": 1,
                "jobs": [
                    {
                        "job_id": "hpc-job-1",
                        "job_name": "hpc-test",
                        "status": "QUEUEING",
                        "workspace_id": workspace_id,
                        "created_by": {"id": "user-1", "name": "张三"},
                    }
                ],
            }

    monkeypatch.setattr(mcp_server, "_require_cookie", lambda: ("cookie=value", {}))
    monkeypatch.setattr(
        mcp_server,
        "_resolve_workspace_refs",
        lambda workspace, all_workspaces=False: [{"id": "ws-1", "name": "CPU资源空间"}],
    )
    monkeypatch.setattr(mcp_server, "get_api", lambda: _FakeAPI())

    result = mcp_server.qz_list_hpc_jobs(
        workspace="CPU资源空间",
        created_by="user-1",
        status="QUEUEING",
        limit=10,
    )

    assert calls == [
        {
            "workspace_id": "ws-1",
            "cookie": "cookie=value",
            "page_size": 10,
            "created_by": "user-1",
            "status": "QUEUEING",
        }
    ]
    assert result["ok"] is True
    assert result["data"]["endpoint"] == "/api/v1/hpc_jobs/list"
    assert result["data"]["raw_total"] == 1
    assert result["data"]["jobs"][0]["created_by_id"] == "user-1"
    assert result["data"]["jobs"][0]["status_raw"] == "QUEUEING"
    assert result["data"]["jobs"][0]["status_family"] == "waiting"


def test_mcp_list_user_jobs_queries_train_and_hpc(monkeypatch):
    calls = []

    class _FakeAPI:
        def list_jobs_with_cookie(
            self,
            workspace_id,
            cookie,
            page_num=1,
            page_size=100,
            created_by=None,
            status=None,
        ):
            calls.append(
                {
                    "endpoint": "train",
                    "workspace_id": workspace_id,
                    "cookie": cookie,
                    "page_num": page_num,
                    "page_size": page_size,
                    "created_by": created_by,
                    "status": status,
                }
            )
            if status != "job_queuing":
                return {"total": 0, "jobs": []}
            return {
                "total": 1,
                "jobs": [
                    {
                        "job_id": "job-1",
                        "job_name": "train-test",
                        "status": "QUEUEING",
                        "workspace_id": workspace_id,
                        "created_by": {"id": "user-1", "name": "张三"},
                    }
                ],
            }

        def list_hpc_jobs_with_cookie(
            self,
            workspace_id,
            cookie,
            page_num=1,
            page_size=100,
            created_by=None,
            status=None,
        ):
            calls.append(
                {
                    "endpoint": "hpc",
                    "workspace_id": workspace_id,
                    "cookie": cookie,
                    "page_num": page_num,
                    "page_size": page_size,
                    "created_by": created_by,
                    "status": status,
                }
            )
            if status == "QUEUEING":
                return {
                    "total": 1,
                    "jobs": [
                        {
                            "job_id": "hpc-job-1",
                            "job_name": "hpc-test",
                            "status": "QUEUEING",
                            "workspace_id": workspace_id,
                            "created_by": {"id": "user-1", "name": "张三"},
                        }
                    ],
                }
            return {"total": 0, "jobs": []}

    monkeypatch.setattr(mcp_server, "_require_cookie", lambda: ("cookie=value", {}))
    monkeypatch.setattr(
        mcp_server,
        "_resolve_workspace_refs",
        lambda workspace, all_workspaces=False: [{"id": "ws-1", "name": "CPU资源空间"}],
    )
    monkeypatch.setattr(mcp_server, "get_api", lambda: _FakeAPI())

    result = mcp_server.qz_list_user_jobs(
        created_by="user-1",
        all_workspaces=True,
        queued_only=True,
        limit=10,
    )

    assert calls == [
        {
            "endpoint": "train",
            "workspace_id": "ws-1",
            "cookie": "cookie=value",
            "page_num": 1,
            "page_size": 100,
            "created_by": "user-1",
            "status": "job_queuing",
        },
        {
            "endpoint": "train",
            "workspace_id": "ws-1",
            "cookie": "cookie=value",
            "page_num": 1,
            "page_size": 100,
            "created_by": "user-1",
            "status": "job_pending",
        },
        {
            "endpoint": "train",
            "workspace_id": "ws-1",
            "cookie": "cookie=value",
            "page_num": 1,
            "page_size": 100,
            "created_by": "user-1",
            "status": "CREATING",
        },
        {
            "endpoint": "hpc",
            "workspace_id": "ws-1",
            "cookie": "cookie=value",
            "page_num": 1,
            "page_size": 100,
            "created_by": "user-1",
            "status": "QUEUEING",
        },
        {
            "endpoint": "hpc",
            "workspace_id": "ws-1",
            "cookie": "cookie=value",
            "page_num": 1,
            "page_size": 100,
            "created_by": "user-1",
            "status": "CREATING",
        },
    ]
    assert result["ok"] is True
    assert result["data"]["matched"] == 2
    assert result["data"]["view_counts"] == {"distributed_training": 1, "hpc": 1}
    assert {job["endpoint"] for job in result["data"]["jobs"]} == {
        "/api/v1/train_job/list",
        "/api/v1/hpc_jobs/list",
    }


def test_pick_workspace_prefers_distributed_training_space_without_cookie():
    workspace_id, workspace_name = task_dimensions._pick_workspace(
        requested_workspace="",
        cookie_workspace_id="",
        workspace_options=[
            {"id": "ws-ppu", "name": "CI-PPU"},
            {"id": "ws-train", "name": "分布式训练空间"},
            {"id": "ws-ci", "name": "CI-情境智能"},
        ],
    )

    assert (workspace_id, workspace_name) == ("ws-train", "分布式训练空间")


def test_create_parser_accepts_auto_fault_tolerance_alias(monkeypatch):
    captured = {}

    def fake_cmd_create(args):
        captured["auto_fault_tolerance"] = args.auto_fault_tolerance
        captured["fault_tolerance_max_retry"] = args.fault_tolerance_max_retry
        captured["command"] = args.command
        return 0

    monkeypatch.setattr(cli, "cmd_create", fake_cmd_create)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "qzcli",
            "create",
            "--name",
            "test-job",
            "--command",
            "echo hi",
            "--workspace",
            "ws-1",
            "--auto_fault_tolerance",
        ],
    )

    assert cli.main() == 0
    assert captured == {
        "auto_fault_tolerance": True,
        "fault_tolerance_max_retry": 3,
        "command": "create",
    }


def test_cmd_create_dry_run_passes_auto_fault_tolerance(monkeypatch, capsys):
    class _FakeDisplay:
        def print(self, *args, **kwargs):
            return None

        def print_error(self, *args, **kwargs):
            return None

    monkeypatch.setattr(create_commands, "get_display", lambda: _FakeDisplay())
    monkeypatch.setattr(create_commands, "get_api", lambda: object())
    monkeypatch.setattr(create_commands, "get_store", lambda: object())
    monkeypatch.setattr(
        create_commands,
        "resolve_create_context",
        lambda args, display: {
            "workspace_id": "ws-1",
            "workspace_display": "空间",
            "project_id": "proj-1",
            "project_display": "项目",
            "compute_group_id": "cg-1",
            "compute_group_display": "计算组",
            "spec_id": "spec-1",
            "spec_display": "规格",
        },
    )

    args = Namespace(
        name="test-job",
        cmd_str="echo hi",
        workspace="ws-1",
        project="proj-1",
        compute_group="cg-1",
        spec="spec-1",
        image="repo/image:latest",
        image_type="SOURCE_PRIVATE",
        instances=2,
        shm=1200,
        priority=10,
        framework="pytorch",
        auto_fault_tolerance=True,
        fault_tolerance_max_retry=5,
        no_track=False,
        dry_run=True,
        output_json=False,
    )

    assert create_commands.cmd_create(args) == 0
    out = capsys.readouterr().out

    assert '"auto_fault_tolerance": true' in out
    assert '"fault_tolerance_max_retry": 5' in out


def test_cmd_create_dry_run_omits_fault_tolerance_retry_when_disabled(monkeypatch, capsys):
    class _FakeDisplay:
        def print(self, *args, **kwargs):
            return None

        def print_error(self, *args, **kwargs):
            return None

    monkeypatch.setattr(create_commands, "get_display", lambda: _FakeDisplay())
    monkeypatch.setattr(create_commands, "get_api", lambda: object())
    monkeypatch.setattr(create_commands, "get_store", lambda: object())
    monkeypatch.setattr(
        create_commands,
        "resolve_create_context",
        lambda args, display: {
            "workspace_id": "ws-1",
            "workspace_display": "空间",
            "project_id": "proj-1",
            "project_display": "项目",
            "compute_group_id": "cg-1",
            "compute_group_display": "计算组",
            "spec_id": "spec-1",
            "spec_display": "规格",
        },
    )

    args = Namespace(
        name="test-job",
        cmd_str="echo hi",
        workspace="ws-1",
        project="proj-1",
        compute_group="cg-1",
        spec="spec-1",
        image="repo/image:latest",
        image_type="SOURCE_PRIVATE",
        instances=2,
        shm=1200,
        priority=10,
        framework="pytorch",
        auto_fault_tolerance=False,
        fault_tolerance_max_retry=7,
        no_track=False,
        dry_run=True,
        output_json=False,
    )

    assert create_commands.cmd_create(args) == 0
    out = capsys.readouterr().out

    assert '"auto_fault_tolerance": false' in out
    assert '"fault_tolerance_max_retry"' not in out


def test_create_hpc_parser_defaults_ttl_after_finish_seconds(monkeypatch):
    captured = {}

    def fake_cmd_create_hpc(args):
        captured["ttl_after_finish_seconds"] = args.ttl_after_finish_seconds
        captured["command"] = args.command
        return 0

    monkeypatch.setattr(cli, "cmd_create_hpc", fake_cmd_create_hpc)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "qzcli",
            "create-hpc",
            "--name",
            "test-hpc",
            "--entrypoint",
            "hostname",
            "--workspace",
            "ws-1",
            "--image",
            "repo/hpc:latest",
            "--memory-per-cpu",
            "4Gi",
        ],
    )

    assert cli.main() == 0
    assert captured == {
        "ttl_after_finish_seconds": 600,
        "command": "create-hpc",
    }


def test_specs_parser_accepts_schedule_config_type(monkeypatch):
    captured = {}

    def fake_cmd_specs(args):
        captured["workspace"] = args.workspace
        captured["group"] = args.group
        captured["all_workspaces"] = args.all_workspaces
        captured["schedule_config_type"] = args.schedule_config_type
        captured["summary"] = args.summary
        captured["output_json"] = args.output_json
        captured["command"] = args.command
        return 0

    monkeypatch.setattr(cli, "cmd_specs", fake_cmd_specs)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "qzcli",
            "specs",
            "--workspace",
            "ws-1",
            "--group",
            "lcg-1",
            "--schedule-config-type",
            "SCHEDULE_CONFIG_TYPE_HPC",
            "--summary",
            "--json",
        ],
    )

    assert cli.main() == 0
    assert captured == {
        "workspace": "ws-1",
        "group": "lcg-1",
        "all_workspaces": False,
        "schedule_config_type": "SCHEDULE_CONFIG_TYPE_HPC",
        "summary": True,
        "output_json": True,
        "command": "specs",
    }


def test_thresholds_from_resource_specs_dedupes_and_sorts():
    specs = [
        {"quota_id": "b", "cpu_count": 55, "memory_size_gib": 500},
        {"quota_id": "a", "cpu_count": "20", "memory_size_gib": "100"},
        {"quota_id": "dup", "cpu_count": 55, "memory_size_gib": 500},
        {"quota_id": "bad-cpu", "cpu_count": "", "memory_size_gib": 100},
        {"quota_id": "bad-mem", "cpu_count": 40, "memory_size_gib": None},
    ]

    assert resource_commands._thresholds_from_resource_specs(specs) == [
        {"cpu": 20.0, "mem": 100.0},
        {"cpu": 55.0, "mem": 500.0},
    ]


def test_collect_dynamic_cpu_thresholds_reads_one_group():
    class _FakeAPI:
        def __init__(self):
            self.calls = []

        def list_resource_spec_prices(
            self, workspace_id, group_id, cookie, schedule_config_type
        ):
            self.calls.append((workspace_id, group_id, cookie, schedule_config_type))
            return [
                {"quota_id": "spec-1", "cpu_count": 40, "memory_size_gib": 200},
                {"quota_id": "spec-2", "cpu_count": 20, "memory_size_gib": 100},
            ]

    class _FakeDisplay:
        def print_warning(self, *args, **kwargs):
            return None

    api = _FakeAPI()
    thresholds = resource_commands._collect_dynamic_cpu_thresholds_for_group(
        api,
        "cookie=value",
        "ws-1",
        "lcg-1",
        "空间",
        "分区",
        _FakeDisplay(),
    )

    assert api.calls == [
        ("ws-1", "lcg-1", "cookie=value", "SCHEDULE_CONFIG_TYPE_HPC")
    ]
    assert thresholds == [
        {"cpu": 20.0, "mem": 100.0},
        {"cpu": 40.0, "mem": 200.0},
    ]


def test_default_cpu_workspace_ids_filters_to_known_spaces():
    resources = {
        "ws-other": {"name": "CI-情境智能"},
        "ws-1177d2a5-aef0-40d3-8777-fed9af13affc": {"name": "CPU临时测试空间"},
        "ws-f9be64cb-9b66-40fb-8172-488abed619bc": {"name": "高性能计算"},
        "ws-6e6ba362-e98e-45b2-9c5a-311998e93d65": {"name": "CPU资源空间"},
    }

    assert resource_commands._default_cpu_workspace_ids(resources) == [
        "ws-6e6ba362-e98e-45b2-9c5a-311998e93d65",
        "ws-f9be64cb-9b66-40fb-8172-488abed619bc",
        "ws-1177d2a5-aef0-40d3-8777-fed9af13affc",
    ]


def test_mcp_list_resource_specs_lists_workspace_groups(monkeypatch):
    class _FakeAPI:
        def __init__(self):
            self.calls = []

        def list_resource_spec_prices(
            self, workspace_id, group_id, cookie, schedule_config_type
        ):
            self.calls.append((workspace_id, group_id, cookie, schedule_config_type))
            return [
                {
                    "quota_id": "quota-1",
                    "cpu_count": 20,
                    "memory_size_gib": 100,
                    "gpu_count": 0,
                    "total_price_per_hour": 0,
                }
            ]

    fake_api = _FakeAPI()
    monkeypatch.setattr(mcp_server, "_require_cookie", lambda: ("cookie=value", {}))
    monkeypatch.setattr(
        mcp_server,
        "_resolve_workspace_refs",
        lambda *args, **kwargs: [{"id": "ws-1", "name": "空间"}],
    )
    monkeypatch.setattr(
        mcp_server,
        "get_workspace_resources",
        lambda workspace_id: {
            "name": "空间",
            "compute_groups": {
                "lcg-1": {"id": "lcg-1", "name": "分区", "gpu_type": "CPU"}
            },
        },
    )
    monkeypatch.setattr(mcp_server, "get_api", lambda: fake_api)

    result = mcp_server.qz_list_resource_specs("空间")

    assert fake_api.calls == [
        ("ws-1", "lcg-1", "cookie=value", "SCHEDULE_CONFIG_TYPE_HPC")
    ]
    assert result["ok"] is True
    assert result["data"]["result_count"] == 1
    specs = result["data"]["results"][0]["specs"]
    assert specs == [
        {
            "spec_id": "quota-1",
            "quota_id": "quota-1",
            "cpu_count": 20,
            "memory_size_gib": 100,
            "gpu_count": 0,
            "gpu_type": "",
            "total_price_per_hour": 0,
        }
    ]


def test_cmd_create_hpc_dry_run_passes_ttl_after_finish_seconds(monkeypatch, capsys):
    class _FakeDisplay:
        def print(self, *args, **kwargs):
            return None

        def print_error(self, *args, **kwargs):
            return None

    monkeypatch.setattr(create_commands, "get_display", lambda: _FakeDisplay())
    monkeypatch.setattr(create_commands, "get_api", lambda: object())
    monkeypatch.setattr(create_commands, "get_store", lambda: object())
    monkeypatch.setattr(
        create_commands,
        "resolve_create_context",
        lambda args, display: {
            "workspace_id": "ws-1",
            "workspace_display": "空间",
            "project_id": "proj-1",
            "project_display": "项目",
            "compute_group_id": "cg-1",
            "compute_group_display": "计算组",
            "spec_id": "spec-1",
            "spec_display": "规格",
        },
    )

    args = Namespace(
        name="test-hpc",
        entrypoint="hostname",
        workspace="ws-1",
        project="proj-1",
        compute_group="cg-1",
        spec="spec-1",
        image="repo/hpc:latest",
        image_type="SOURCE_PRIVATE",
        instances=2,
        number_of_tasks=16,
        cpus_per_task=8,
        memory_per_cpu="8Gi",
        ttl_after_finish_seconds=900,
        enable_hyper_threading=False,
        track=False,
        dry_run=True,
        output_json=False,
    )

    assert create_commands.cmd_create_hpc(args) == 0
    out = capsys.readouterr().out

    assert '"ttl_after_finish_seconds": 900' in out
