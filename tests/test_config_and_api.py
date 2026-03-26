import pytest

import qzcli.config as config
from qzcli.api import QzAPI, QzAPIError
from qzcli.resource_resolution import ResourceResolutionError, resolve_workspace_ref


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
