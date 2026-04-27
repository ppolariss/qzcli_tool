import pytest

import qzcli.config as config
from qzcli.api import QzAPI, QzAPIError
from qzcli.resource_resolution import ResourceResolutionError, resolve_workspace_ref
from qzcli.store import JobRecord


@pytest.fixture()
def isolated_config_paths(tmp_path, monkeypatch):
    config_dir = tmp_path / ".qzcli"
    monkeypatch.setattr(config, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(config, "RESOURCES_FILE", config_dir / "resources.json")
    monkeypatch.setattr(config, "WORKSPACE_ALIASES_FILE", config_dir / "workspace_aliases.json")
    monkeypatch.setattr(config, "CONFIG_FILE", config_dir / "config.json")
    monkeypatch.setattr(config, "TOKEN_CACHE_FILE", config_dir / ".token_cache")
    monkeypatch.setattr(config, "COOKIE_FILE", config_dir / ".cookie")
    monkeypatch.setattr(config, "JOBS_FILE", config_dir / "jobs.json")
    return config_dir


def test_workspace_alias_is_separate_from_resource_snapshot(isolated_config_paths):
    config.save_resources(
        "ws-1",
        {"projects": [], "compute_groups": [], "specs": []},
        name="CI-情境智能",
    )
    config.set_workspace_name("ws-1", "我的空间")

    first = config.get_workspace_resources("ws-1")
    assert first is not None
    assert first["name"] == "我的空间"
    assert first["official_name"] == "CI-情境智能"
    assert first["alias"] == "我的空间"

    config.save_resources(
        "ws-1",
        {"projects": [], "compute_groups": [], "specs": []},
        name="CI-情境智能-新",
    )

    second = config.get_workspace_resources("ws-1")
    assert second is not None
    assert second["name"] == "我的空间"
    assert second["official_name"] == "CI-情境智能-新"
    assert second["alias"] == "我的空间"

    assert resolve_workspace_ref("我的空间") == ("ws-1", "我的空间")
    assert resolve_workspace_ref("CI-情境智能-新") == ("ws-1", "我的空间")


class _FakeResponse:
    def __init__(self, *, status_code=200, payload=None, json_error: Exception | None = None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"code": 0, "data": {}}
        self._json_error = json_error

    def json(self):
        if self._json_error:
            raise self._json_error
        return self._payload


def test_list_workspaces_uses_shared_cookie_request(monkeypatch):
    captured = {}

    def fake_post(url, json, headers, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _FakeResponse(
            payload={
                "code": 0,
                "data": {
                    "items": [
                        {"space_list": [{"id": "ws-1", "name": "CI-情境智能"}]},
                        {"space_list": [{"id": "ws-1", "name": "CI-情境智能"}]},
                    ]
                },
            }
        )

    monkeypatch.setattr("requests.post", fake_post)

    api = QzAPI(username="u", password="p")
    workspaces = api.list_workspaces("cookie=value")

    assert workspaces == [{"id": "ws-1", "name": "CI-情境智能"}]
    assert captured["url"].endswith("/api/v1/project/list")
    assert captured["headers"]["cookie"] == "cookie=value"
    assert captured["headers"]["referer"] == "https://qz.sii.edu.cn/operations/projects"
    assert captured["timeout"] == 60


def test_list_resource_spec_prices_uses_resource_prices_endpoint(monkeypatch):
    captured = {}

    def fake_post(url, json, headers, timeout):
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _FakeResponse(
            payload={
                "code": 0,
                "data": {
                    "lcg_resource_spec_prices": [
                        {
                            "quota_id": "quota-1",
                            "cpu_count": 20,
                            "memory_size_gib": 100,
                            "gpu_count": 0,
                        }
                    ]
                },
            }
        )

    monkeypatch.setattr("requests.post", fake_post)

    api = QzAPI(username="u", password="p")
    specs = api.list_resource_spec_prices(
        "ws-1",
        "lcg-1",
        "cookie=value",
        schedule_config_type="SCHEDULE_CONFIG_TYPE_HPC",
    )

    assert specs == [
        {
            "quota_id": "quota-1",
            "cpu_count": 20,
            "memory_size_gib": 100,
            "gpu_count": 0,
        }
    ]
    assert captured["url"].endswith("/api/v1/resource_prices/logic_compute_groups/")
    assert captured["json"] == {
        "workspace_id": "ws-1",
        "logic_compute_group_id": "lcg-1",
        "schedule_config_type": "SCHEDULE_CONFIG_TYPE_HPC",
    }
    assert captured["headers"]["cookie"] == "cookie=value"
    assert captured["headers"]["referer"] == "https://qz.sii.edu.cn/jobs/create?spaceId=ws-1"
    assert captured["timeout"] == 60


def test_job_detail_routes_hpc_job_ids_to_hpc_endpoint(monkeypatch):
    captured = {}

    def fake_post(url, json, headers, timeout):
        captured["url"] = url
        captured["json"] = json
        return _FakeResponse(payload={"code": 0, "data": {"job_id": "hpc-job-1"}})

    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr(QzAPI, "_get_token", lambda self: "token")

    api = QzAPI(username="u", password="p")
    assert api.get_job_detail("hpc-job-1") == {"job_id": "hpc-job-1"}
    assert captured["url"].endswith("/openapi/v1/hpc_jobs/detail")
    assert captured["json"] == {"job_id": "hpc-job-1"}


def test_job_detail_keeps_train_jobs_on_train_endpoint(monkeypatch):
    captured = {}

    def fake_post(url, json, headers, timeout):
        captured["url"] = url
        captured["json"] = json
        return _FakeResponse(payload={"code": 0, "data": {"job_id": "job-1"}})

    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr(QzAPI, "_get_token", lambda self: "token")

    api = QzAPI(username="u", password="p")
    assert api.get_job_detail("job-1") == {"job_id": "job-1"}
    assert captured["url"].endswith("/openapi/v1/train_job/detail")
    assert captured["json"] == {"job_id": "job-1"}


def test_stop_routes_hpc_job_ids_to_hpc_endpoint(monkeypatch):
    captured = {}

    def fake_post(url, json, headers, timeout):
        captured["url"] = url
        captured["json"] = json
        return _FakeResponse(payload={"code": 0, "data": {"job_id": "hpc-job-1", "sub_code": 0}})

    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr(QzAPI, "_get_token", lambda self: "token")

    api = QzAPI(username="u", password="p")
    result = api.stop_job_result("hpc-job-1")

    assert result["stopped"] is True
    assert result["data"] == {"job_id": "hpc-job-1", "sub_code": 0}
    assert captured["url"].endswith("/openapi/v1/hpc_jobs/stop")
    assert captured["json"] == {"job_id": "hpc-job-1"}


def test_stop_hpc_job_treats_terminal_detail_as_success(monkeypatch):
    calls = []

    def fake_post(url, json, headers, timeout):
        calls.append(url)
        if url.endswith("/openapi/v1/hpc_jobs/stop"):
            return _FakeResponse(status_code=500)
        if url.endswith("/openapi/v1/hpc_jobs/detail"):
            return _FakeResponse(payload={"code": 0, "data": {"job_id": "hpc-job-1", "status": "STOPPED"}})
        raise AssertionError(url)

    monkeypatch.setattr("requests.post", fake_post)
    monkeypatch.setattr(QzAPI, "_get_token", lambda self: "token")

    api = QzAPI(username="u", password="p")
    result = api.stop_job_result("hpc-job-1")

    assert result["stopped"] is True
    assert result["data"] == {
        "job_id": "hpc-job-1",
        "already_terminal": True,
        "status": "STOPPED",
    }
    assert calls[0].endswith("/openapi/v1/hpc_jobs/stop")
    assert calls[1].endswith("/openapi/v1/hpc_jobs/detail")


def test_hpc_api_response_is_normalized_for_local_store():
    job = JobRecord.from_api_response(
        {
            "job_id": "hpc-job-1",
            "job_name": "hpc-test",
            "status": "QUEUEING",
            "workspace_id": "ws-1",
            "project_id": "project-1",
            "project_name": "项目",
            "logic_compute_group_name": "HPC 分区",
            "created_at": "2026-04-27 16:49:50",
            "sbatch_script": {"entrypoint": "hostname", "number_of_tasks": 1},
            "slurm_cluster_spec": {"instance_count": 1},
            "resource_spec_price": {"gpu_count": 0, "gpu_info": {"gpu_type_display": "CPU"}},
        }
    )

    assert job.name == "hpc-test"
    assert job.status == "job_queuing"
    assert job.created_at == "2026-04-27T16:49:50"
    assert job.command == "hostname"
    assert job.instance_count == 1
    assert job.gpu_type == "CPU"


def test_list_workspaces_paginates_all_pages(monkeypatch):
    calls = []

    def fake_post(url, json, headers, timeout):
        calls.append(json["page"])
        if json["page"] == 1:
            items = [{"space_list": [{"id": "ws-1", "name": "CI-情境智能"}]}] * 100
            return _FakeResponse(payload={"code": 0, "data": {"items": items, "total": 101}})
        return _FakeResponse(
            payload={
                "code": 0,
                "data": {"items": [{"space_list": [{"id": "ws-2", "name": "CI-新空间"}]}], "total": 101},
            }
        )

    monkeypatch.setattr("requests.post", fake_post)

    api = QzAPI(username="u", password="p")
    workspaces = api.list_workspaces("cookie=value")

    assert calls == [1, 2]
    assert workspaces == [
        {"id": "ws-1", "name": "CI-情境智能"},
        {"id": "ws-2", "name": "CI-新空间"},
    ]


def test_cookie_request_raises_on_unauthorized(monkeypatch):
    monkeypatch.setattr(
        "requests.post",
        lambda *args, **kwargs: _FakeResponse(status_code=401),
    )

    api = QzAPI(username="u", password="p")
    api.ensure_cookie = lambda force_refresh=False: (_ for _ in ()).throw(
        QzAPIError("Cookie 已过期或无效，请重新获取", 401)
    )

    with pytest.raises(QzAPIError) as excinfo:
        api.list_workspaces("cookie=value")

    assert excinfo.value.code == 401
    assert "Cookie 已过期或无效" in str(excinfo.value)


def test_ensure_cookie_refreshes_with_config_credentials(isolated_config_paths):
    config.init_config("user-a", "pass-a")

    api = QzAPI()

    original_login = api.login_with_cas
    captured = {}

    def fake_login(username, password):
        captured["username"] = username
        captured["password"] = password
        return "session=new-cookie"

    api.login_with_cas = fake_login
    try:
        cookie_data = api.ensure_cookie()
    finally:
        api.login_with_cas = original_login

    assert cookie_data["cookie"] == "session=new-cookie"
    assert captured == {"username": "user-a", "password": "pass-a"}
    assert config.get_cookie()["cookie"] == "session=new-cookie"


def test_cookie_request_refreshes_and_retries_on_unauthorized(isolated_config_paths, monkeypatch):
    config.init_config("user-b", "pass-b")
    config.save_cookie("session=stale", "ws-1")

    calls = {"count": 0, "cookies": []}

    def fake_post(url, json, headers, timeout):
        calls["count"] += 1
        calls["cookies"].append(headers.get("cookie"))
        if calls["count"] == 1:
            return _FakeResponse(status_code=401)
        return _FakeResponse(
            payload={
                "code": 0,
                "data": {"items": [{"space_list": [{"id": "ws-1", "name": "CI-情境智能"}]}]},
            }
        )

    monkeypatch.setattr("requests.post", fake_post)

    api = QzAPI()

    original_login = api.login_with_cas

    def fake_login(username, password):
        assert username == "user-b"
        assert password == "pass-b"
        return "session=fresh"

    api.login_with_cas = fake_login
    try:
        workspaces = api.list_workspaces("session=stale")
    finally:
        api.login_with_cas = original_login

    assert workspaces == [{"id": "ws-1", "name": "CI-情境智能"}]
    assert calls["cookies"] == ["session=stale", "session=fresh"]
    refreshed = config.get_cookie()
    assert refreshed is not None
    assert refreshed["cookie"] == "session=fresh"
    assert refreshed["workspace_id"] == "ws-1"


def test_find_workspace_by_name_raises_on_ambiguous(isolated_config_paths):
    config.save_resources("ws-1", {"projects": [], "compute_groups": [], "specs": []}, name="CI-情境智能")
    config.save_resources("ws-2", {"projects": [], "compute_groups": [], "specs": []}, name="CI-情境智能-国产卡")

    with pytest.raises(ResourceResolutionError):
        config.find_workspace_by_name("情境智能")


def test_find_resource_by_name_raises_on_ambiguous(isolated_config_paths):
    config.save_resources(
        "ws-1",
        {
            "projects": [],
            "compute_groups": [
                {"id": "lcg-1", "name": "MOVA-Audio"},
                {"id": "lcg-2", "name": "MOVA-Audio-debug"},
            ],
            "specs": [],
        },
        name="CI-情境智能",
    )

    with pytest.raises(ResourceResolutionError):
        config.find_resource_by_name("ws-1", "compute_groups", "MOVA")
