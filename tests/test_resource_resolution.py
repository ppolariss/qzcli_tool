from pathlib import Path
import json

import pytest

import qzcli.config as config
from qzcli.resource_resolution import (
    ResourceResolutionError,
    resolve_cached_resource_ref,
    resolve_create_refs,
    resolve_workspace_ref,
)


@pytest.fixture()
def isolated_config_dir(tmp_path, monkeypatch):
    config_dir = tmp_path / ".qzcli"
    resources_file = config_dir / "resources.json"

    monkeypatch.setattr(config, "CONFIG_DIR", config_dir)
    monkeypatch.setattr(config, "RESOURCES_FILE", resources_file)

    return config_dir, resources_file


def write_resources(resources_file: Path, data: dict) -> None:
    resources_file.parent.mkdir(parents=True, exist_ok=True)
    resources_file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def test_resolve_workspace_ref_rejects_ambiguous_names(isolated_config_dir):
    _, resources_file = isolated_config_dir
    write_resources(
        resources_file,
        {
            "ws-1": {"id": "ws-1", "name": "CI-情境智能", "projects": {}, "compute_groups": {}, "specs": {}},
            "ws-2": {"id": "ws-2", "name": "CI-情境智能-国产卡", "projects": {}, "compute_groups": {}, "specs": {}},
            "ws-3": {"id": "ws-3", "name": "CI-情境智能-国产卡-ssd3", "projects": {}, "compute_groups": {}, "specs": {}},
        },
    )

    with pytest.raises(ResourceResolutionError) as excinfo:
        resolve_workspace_ref("情境智能")

    message = str(excinfo.value)
    assert "匹配到多个结果" in message
    assert "ws-1" in message
    assert "ws-2" in message
    assert "ws-3" in message


def test_resolve_cached_resource_ref_rejects_ambiguous_compute_groups(isolated_config_dir):
    _, resources_file = isolated_config_dir
    write_resources(
        resources_file,
        {
            "ws-1": {
                "id": "ws-1",
                "name": "CI-情境智能",
                "projects": {},
                "compute_groups": {
                    "lcg-1": {"id": "lcg-1", "name": "MOVA-Audio"},
                    "lcg-2": {"id": "lcg-2", "name": "MOVA-Audio-debug"},
                },
                "specs": {},
            }
        },
    )

    with pytest.raises(ResourceResolutionError) as excinfo:
        resolve_cached_resource_ref("ws-1", "compute_groups", "MOVA")

    message = str(excinfo.value)
    assert "计算组 'MOVA' 匹配到多个结果" in message
    assert "lcg-1" in message
    assert "lcg-2" in message


def test_resolve_create_refs_resolves_spec_name_to_id(isolated_config_dir):
    _, resources_file = isolated_config_dir
    write_resources(
        resources_file,
        {
            "ws-1": {
                "id": "ws-1",
                "name": "CI-情境智能",
                "projects": {
                    "project-1": {"id": "project-1", "name": "扩散"},
                },
                "compute_groups": {
                    "lcg-1": {"id": "lcg-1", "name": "MOVA-Audio"},
                },
                "specs": {
                    "spec-1": {"id": "spec-1", "name": "8xH100"},
                },
            }
        },
    )

    ctx = resolve_create_refs(
        workspace="CI-情境智能",
        project="扩散",
        compute_group="MOVA-Audio",
        spec="8xH100",
    )

    assert ctx.workspace_id == "ws-1"
    assert ctx.project_id == "project-1"
    assert ctx.compute_group_id == "lcg-1"
    assert ctx.spec_id == "spec-1"
    assert ctx.spec_display == "8xH100"
    assert not ctx.auto_project
    assert not ctx.auto_compute_group
    assert not ctx.auto_spec


def test_resolve_create_refs_marks_auto_selected_resources(isolated_config_dir):
    _, resources_file = isolated_config_dir
    write_resources(
        resources_file,
        {
            "ws-1": {
                "id": "ws-1",
                "name": "CI-情境智能",
                "projects": {
                    "project-1": {"id": "project-1", "name": "扩散"},
                },
                "compute_groups": {
                    "lcg-1": {"id": "lcg-1", "name": "MOVA-Audio"},
                },
                "specs": {
                    "spec-1": {"id": "spec-1", "name": "8xH100"},
                },
            }
        },
    )

    ctx = resolve_create_refs(workspace="CI-情境智能")

    assert ctx.project_id == "project-1"
    assert ctx.compute_group_id == "lcg-1"
    assert ctx.spec_id == "spec-1"
    assert ctx.auto_project
    assert ctx.auto_compute_group
    assert ctx.auto_spec


def test_resolve_create_refs_requires_explicit_compute_group_when_multiple_candidates(isolated_config_dir):
    _, resources_file = isolated_config_dir
    write_resources(
        resources_file,
        {
            "ws-1": {
                "id": "ws-1",
                "name": "CI-情境智能",
                "projects": {
                    "project-1": {"id": "project-1", "name": "扩散"},
                },
                "compute_groups": {
                    "lcg-1": {"id": "lcg-1", "name": "MOVA-Audio"},
                    "lcg-2": {"id": "lcg-2", "name": "MOVA-Video"},
                },
                "specs": {
                    "spec-1": {"id": "spec-1", "name": "8xH100"},
                },
            }
        },
    )

    with pytest.raises(ResourceResolutionError) as excinfo:
        resolve_create_refs(
            workspace="CI-情境智能",
            project="扩散",
            spec="8xH100",
        )

    message = str(excinfo.value)
    assert "未指定计算组" in message
    assert "MOVA-Audio" in message
    assert "MOVA-Video" in message
