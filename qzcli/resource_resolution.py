"""
资源解析辅助逻辑。

将基于缓存的 workspace / project / compute group / spec 解析收口到一处，
避免 CLI 与 MCP 维护多份相似但逐渐漂移的实现。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from .config import get_workspace_resources, load_all_resources


RESOURCE_TYPE_LABELS = {
    "projects": "项目",
    "compute_groups": "计算组",
    "specs": "资源规格",
}

RESOURCE_ID_PREFIXES = {
    "projects": "project-",
    "compute_groups": "lcg-",
}


class ResourceResolutionError(Exception):
    """资源解析失败。"""


@dataclass(frozen=True)
class ResourceMatch:
    id: str
    name: str
    alternate_names: tuple[str, ...] = ()

    @property
    def display_name(self) -> str:
        return self.name or self.id


@dataclass(frozen=True)
class ResolvedCreateContext:
    workspace_id: str
    workspace_display: str
    project_id: str
    project_display: str
    compute_group_id: str
    compute_group_display: str
    spec_id: str
    spec_display: str
    auto_project: bool = False
    auto_compute_group: bool = False
    auto_spec: bool = False


def _workspace_match_from_cache(workspace_id: str, workspace_data: Dict[str, Any]) -> ResourceMatch:
    alternates = []
    for candidate in (workspace_data.get("official_name", ""), workspace_data.get("alias", "")):
        if candidate and candidate != workspace_data.get("name", ""):
            alternates.append(candidate)
    return ResourceMatch(
        id=workspace_id,
        name=workspace_data.get("name", ""),
        alternate_names=tuple(alternates),
    )


def _resource_match_from_cache(resource_id: str, resource_data: Dict[str, Any]) -> ResourceMatch:
    return ResourceMatch(id=resource_id, name=resource_data.get("name", ""))


def _format_ambiguous_message(label: str, query: str, matches: Iterable[ResourceMatch]) -> str:
    lines = [f"{label} '{query}' 匹配到多个结果:"]
    for match in matches:
        lines.append(f"- {match.display_name} ({match.id})")
    lines.append("请使用更精确的名称或直接传 ID。")
    return "\n".join(lines)


def _resolve_match_by_name(matches: list[ResourceMatch], query: str, label: str) -> ResourceMatch:
    exact_matches = [
        match for match in matches
        if query == match.name or query in match.alternate_names
    ]
    if len(exact_matches) == 1:
        return exact_matches[0]
    if len(exact_matches) > 1:
        raise ResourceResolutionError(_format_ambiguous_message(label, query, exact_matches))

    normalized_query = query.lower()
    fuzzy_matches = [
        match for match in matches
        if (
            (match.name and normalized_query in match.name.lower())
            or any(normalized_query in candidate.lower() for candidate in match.alternate_names)
        )
    ]
    if len(fuzzy_matches) == 1:
        return fuzzy_matches[0]
    if len(fuzzy_matches) > 1:
        raise ResourceResolutionError(_format_ambiguous_message(label, query, fuzzy_matches))

    raise ResourceResolutionError(f"未找到{label} '{query}'")


def _format_required_selection_message(label: str, matches: Iterable[ResourceMatch]) -> str:
    match_list = list(matches)
    lines = [f"未指定{label}，当前缓存中有 {len(match_list)} 个候选，请显式指定。"]
    for match in match_list[:10]:
        lines.append(f"- {match.display_name} ({match.id})")
    if len(match_list) > 10:
        lines.append(f"- ... 还有 {len(match_list) - 10} 个候选")
    return "\n".join(lines)


def resolve_workspace_ref(workspace: str) -> tuple[str, str]:
    """解析工作空间名称或 ID。"""
    if not workspace:
        raise ResourceResolutionError("请指定工作空间")

    if workspace.startswith("ws-"):
        ws_resources = get_workspace_resources(workspace)
        return workspace, (ws_resources or {}).get("name", workspace)

    all_resources = load_all_resources()
    matches = [
        _workspace_match_from_cache(workspace_id, workspace_data)
        for workspace_id, workspace_data in all_resources.items()
    ]
    resolved = _resolve_match_by_name(matches, workspace, "工作空间")
    return resolved.id, resolved.display_name


def resolve_cached_resource_ref(workspace_id: str, resource_type: str, value: str) -> tuple[str, str]:
    """解析工作空间内缓存的资源名称或 ID。"""
    if not value:
        raise ResourceResolutionError(f"请指定{RESOURCE_TYPE_LABELS.get(resource_type, resource_type)}")

    ws_resources = get_workspace_resources(workspace_id) or {}
    resources = ws_resources.get(resource_type, {})
    label = RESOURCE_TYPE_LABELS.get(resource_type, resource_type)

    if value in resources:
        match = _resource_match_from_cache(value, resources[value])
        return match.id, match.display_name

    prefix = RESOURCE_ID_PREFIXES.get(resource_type, "")
    if prefix and value.startswith(prefix):
        return value, value

    if resource_type == "specs" and (value.count("-") >= 4 or len(value) > 20):
        return value, resources.get(value, {}).get("name", value)

    matches = [
        _resource_match_from_cache(resource_id, resource_data)
        for resource_id, resource_data in resources.items()
    ]
    resolved = _resolve_match_by_name(matches, value, label)
    return resolved.id, resolved.display_name


def auto_select_cached_resource(workspace_id: str, resource_type: str) -> tuple[Optional[str], Optional[str]]:
    """仅在缓存中存在唯一候选时自动选择资源。"""
    ws_resources = get_workspace_resources(workspace_id) or {}
    resources = ws_resources.get(resource_type, {})
    if not resources:
        return None, None

    matches = [
        _resource_match_from_cache(resource_id, resource_data)
        for resource_id, resource_data in resources.items()
    ]
    if len(matches) > 1:
        label = RESOURCE_TYPE_LABELS.get(resource_type, resource_type)
        raise ResourceResolutionError(_format_required_selection_message(label, matches))

    first = matches[0]
    return first.id, first.display_name


def resolve_create_refs(
    *,
    workspace: str,
    project: str = "",
    compute_group: str = "",
    spec: str = "",
) -> ResolvedCreateContext:
    """解析 create/create-hpc 共用的资源上下文。"""
    workspace_id, workspace_display = resolve_workspace_ref(workspace)

    auto_project = False
    if project:
        project_id, project_display = resolve_cached_resource_ref(workspace_id, "projects", project)
    else:
        project_id, project_display = auto_select_cached_resource(workspace_id, "projects")
        if not project_id:
            raise ResourceResolutionError("未指定项目且缓存中无可用项目")
        auto_project = True

    auto_compute_group = False
    if compute_group:
        compute_group_id, compute_group_display = resolve_cached_resource_ref(
            workspace_id, "compute_groups", compute_group
        )
    else:
        compute_group_id, compute_group_display = auto_select_cached_resource(workspace_id, "compute_groups")
        if not compute_group_id:
            raise ResourceResolutionError("未指定计算组且缓存中无可用计算组")
        auto_compute_group = True

    auto_spec = False
    if spec:
        spec_id, spec_display = resolve_cached_resource_ref(workspace_id, "specs", spec)
    else:
        spec_id, spec_display = auto_select_cached_resource(workspace_id, "specs")
        if not spec_id:
            raise ResourceResolutionError("未指定资源规格且缓存中无可用规格")
        auto_spec = True

    return ResolvedCreateContext(
        workspace_id=workspace_id,
        workspace_display=workspace_display,
        project_id=project_id,
        project_display=project_display or project_id,
        compute_group_id=compute_group_id,
        compute_group_display=compute_group_display or compute_group_id,
        spec_id=spec_id,
        spec_display=spec_display or spec_id,
        auto_project=auto_project,
        auto_compute_group=auto_compute_group,
        auto_spec=auto_spec,
    )
