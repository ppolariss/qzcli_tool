"""
配置管理模块
"""

import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List

# 默认配置
DEFAULT_CONFIG = {
    "api_base_url": "https://qz.sii.edu.cn",
    "username": "",
    "password": "",
    "token_cache_enabled": True,
}

# 配置目录
CONFIG_DIR = Path.home() / ".qzcli"
CONFIG_FILE = CONFIG_DIR / "config.json"
JOBS_FILE = CONFIG_DIR / "jobs.json"
TOKEN_CACHE_FILE = CONFIG_DIR / ".token_cache"
COOKIE_FILE = CONFIG_DIR / ".cookie"


def ensure_config_dir() -> Path:
    """确保配置目录存在"""
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    return CONFIG_DIR


def load_config() -> Dict[str, Any]:
    """加载配置文件"""
    ensure_config_dir()
    
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                config = json.load(f)
                # 合并默认配置
                return {**DEFAULT_CONFIG, **config}
        except (json.JSONDecodeError, IOError):
            pass
    
    return DEFAULT_CONFIG.copy()


def save_config(config: Dict[str, Any]) -> None:
    """保存配置文件"""
    ensure_config_dir()
    
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def get_credentials() -> tuple[str, str]:
    """获取认证信息，优先使用环境变量"""
    config = load_config()
    
    username = os.environ.get("QZCLI_USERNAME") or config.get("username") or ""
    password = os.environ.get("QZCLI_PASSWORD") or config.get("password") or ""
    
    return username, password


def get_api_base_url() -> str:
    """获取 API 基础 URL"""
    config = load_config()
    return os.environ.get("QZCLI_API_URL") or config.get("api_base_url", DEFAULT_CONFIG["api_base_url"])


def init_config(username: str, password: str, api_base_url: Optional[str] = None) -> None:
    """初始化配置"""
    config = load_config()
    config["username"] = username
    config["password"] = password
    if api_base_url:
        config["api_base_url"] = api_base_url
    save_config(config)


def get_token_cache() -> Optional[Dict[str, Any]]:
    """获取缓存的 token"""
    if not TOKEN_CACHE_FILE.exists():
        return None
    
    try:
        with open(TOKEN_CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
            # 检查是否过期（预留 5 分钟缓冲）
            import time
            if cache.get("expires_at", 0) > time.time() + 300:
                return cache
    except (json.JSONDecodeError, IOError):
        pass
    
    return None


def save_token_cache(token: str, expires_in: int) -> None:
    """保存 token 缓存"""
    ensure_config_dir()
    
    import time
    cache = {
        "token": token,
        "expires_at": time.time() + expires_in,
    }
    
    with open(TOKEN_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f)


def clear_token_cache() -> None:
    """清除 token 缓存"""
    if TOKEN_CACHE_FILE.exists():
        TOKEN_CACHE_FILE.unlink()


def save_cookie(cookie: str, workspace_id: str = "") -> None:
    """保存浏览器 cookie"""
    ensure_config_dir()
    
    import time
    data = {
        "cookie": cookie,
        "workspace_id": workspace_id,
        "saved_at": time.time(),
    }
    
    with open(COOKIE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def get_cookie() -> Optional[Dict[str, Any]]:
    """获取保存的 cookie"""
    if not COOKIE_FILE.exists():
        return None
    
    try:
        with open(COOKIE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def clear_cookie() -> None:
    """清除 cookie"""
    if COOKIE_FILE.exists():
        COOKIE_FILE.unlink()


# 资源缓存文件
RESOURCES_FILE = CONFIG_DIR / "resources.json"
WORKSPACE_ALIASES_FILE = CONFIG_DIR / "workspace_aliases.json"


def _load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return default


def _save_json_file(path: Path, data: Any) -> None:
    ensure_config_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_workspace_aliases() -> Dict[str, str]:
    """加载工作空间别名。"""
    raw = _load_json_file(WORKSPACE_ALIASES_FILE, {})
    if not isinstance(raw, dict):
        return {}
    return {
        str(workspace_id): str(alias).strip()
        for workspace_id, alias in raw.items()
        if str(alias).strip()
    }


def save_workspace_aliases(aliases: Dict[str, str]) -> None:
    """保存工作空间别名。"""
    sanitized = {
        str(workspace_id): str(alias).strip()
        for workspace_id, alias in aliases.items()
        if str(alias).strip()
    }
    _save_json_file(WORKSPACE_ALIASES_FILE, sanitized)


def _load_raw_resources() -> Dict[str, Any]:
    """加载资源快照原始数据，不叠加别名。"""
    raw = _load_json_file(RESOURCES_FILE, {})
    if not isinstance(raw, dict):
        return {}
    return raw


def _normalize_workspace_snapshot(workspace_id: str, workspace_data: Dict[str, Any], aliases: Dict[str, str]) -> Dict[str, Any]:
    official_name = workspace_data.get("official_name")
    if official_name is None:
        official_name = workspace_data.get("name", "")

    alias = aliases.get(workspace_id, "")
    display_name = alias or official_name or workspace_data.get("id", workspace_id)

    normalized = dict(workspace_data)
    normalized["id"] = workspace_id
    normalized["official_name"] = official_name
    normalized["alias"] = alias
    normalized["name"] = display_name
    return normalized


def _prepare_workspace_snapshot(
    workspace_id: str,
    existing: Optional[Dict[str, Any]] = None,
    *,
    official_name: str = "",
) -> Dict[str, Any]:
    base = dict(existing or {})
    if official_name:
        base["official_name"] = official_name
    else:
        base["official_name"] = base.get("official_name", base.get("name", ""))

    base["id"] = workspace_id
    base["projects"] = dict(base.get("projects", {}))
    base["compute_groups"] = dict(base.get("compute_groups", {}))
    base["specs"] = dict(base.get("specs", {}))
    return base


def save_resources(workspace_id: str, resources: Dict[str, Any], name: str = "") -> None:
    """
    保存工作空间的资源配置到本地缓存
    
    Args:
        workspace_id: 工作空间 ID
        resources: 资源配置（projects, compute_groups, specs）
        name: 工作空间名称（可选）
    """
    import time

    all_resources = _load_raw_resources()
    existing = all_resources.get(workspace_id, {})

    all_resources[workspace_id] = {
        "id": workspace_id,
        "official_name": name or existing.get("official_name", existing.get("name", "")),
        "projects": {p["id"]: p for p in resources.get("projects", [])},
        "compute_groups": {g["id"]: g for g in resources.get("compute_groups", [])},
        "specs": {s["id"]: s for s in resources.get("specs", [])},
        "updated_at": time.time(),
    }

    _save_json_file(RESOURCES_FILE, all_resources)


def load_all_resources() -> Dict[str, Any]:
    """加载所有工作空间的资源缓存"""
    raw_resources = _load_raw_resources()
    aliases = load_workspace_aliases()
    return {
        workspace_id: _normalize_workspace_snapshot(workspace_id, workspace_data, aliases)
        for workspace_id, workspace_data in raw_resources.items()
    }


def get_workspace_resources(workspace_id: str) -> Optional[Dict[str, Any]]:
    """
    获取指定工作空间的资源缓存
    
    Args:
        workspace_id: 工作空间 ID
        
    Returns:
        资源配置字典，或 None（未缓存）
    """
    all_resources = load_all_resources()
    return all_resources.get(workspace_id)


def set_workspace_name(workspace_id: str, name: str) -> bool:
    """
    设置工作空间的名称（别名）
    
    Args:
        workspace_id: 工作空间 ID
        name: 名称
        
    Returns:
        是否成功
    """
    aliases = load_workspace_aliases()
    normalized_name = name.strip()

    if normalized_name:
        aliases[workspace_id] = normalized_name
    else:
        aliases.pop(workspace_id, None)

    save_workspace_aliases(aliases)
    return True


def find_workspace_by_name(name: str) -> Optional[str]:
    """
    通过名称查找工作空间 ID。

    使用统一的资源解析逻辑；当名称歧义时抛出 ResourceResolutionError。
    
    Args:
        name: 工作空间名称（支持唯一精确/模糊匹配）
        
    Returns:
        工作空间 ID，或 None
    """
    from .resource_resolution import ResourceResolutionError, resolve_workspace_ref

    try:
        workspace_id, _ = resolve_workspace_ref(name)
        return workspace_id
    except ResourceResolutionError as exc:
        if str(exc).startswith("未找到"):
            return None
        raise


def find_resource_by_name(
    workspace_id: str,
    resource_type: str,
    name: str
) -> Optional[Dict[str, Any]]:
    """
    通过名称查找资源（项目、计算组、规格）。

    使用统一的资源解析逻辑；当名称歧义时抛出 ResourceResolutionError。
    
    Args:
        workspace_id: 工作空间 ID
        resource_type: 资源类型 (projects, compute_groups, specs)
        name: 资源名称（支持唯一精确/模糊匹配）
        
    Returns:
        资源配置字典，或 None
    """
    from .resource_resolution import ResourceResolutionError, resolve_cached_resource_ref

    ws_resources = get_workspace_resources(workspace_id)
    if not ws_resources:
        return None

    try:
        resource_id, _ = resolve_cached_resource_ref(workspace_id, resource_type, name)
    except ResourceResolutionError as exc:
        if str(exc).startswith("未找到"):
            return None
        raise

    return ws_resources.get(resource_type, {}).get(resource_id)


def list_cached_workspaces() -> List[Dict[str, Any]]:
    """
    列出所有已缓存的工作空间
    
    Returns:
        工作空间列表 [{id, name, updated_at, ...}, ...]
    """
    all_resources = load_all_resources()
    result = []
    
    for ws_id, ws_data in all_resources.items():
        result.append({
            "id": ws_id,
            "name": ws_data.get("name", ""),
            "official_name": ws_data.get("official_name", ""),
            "alias": ws_data.get("alias", ""),
            "updated_at": ws_data.get("updated_at", 0),
            "project_count": len(ws_data.get("projects", {})),
            "compute_group_count": len(ws_data.get("compute_groups", {})),
            "spec_count": len(ws_data.get("specs", {})),
        })
    
    return result


def update_workspace_projects(workspace_id: str, projects: List[Dict[str, Any]], name: str = "") -> int:
    """
    增量更新工作空间的项目列表
    
    Args:
        workspace_id: 工作空间 ID
        projects: 项目列表 [{"id": ..., "name": ...}, ...]
        name: 工作空间名称（可选）
        
    Returns:
        新增的项目数量
    """
    import time

    all_resources = _load_raw_resources()
    ws_data = _prepare_workspace_snapshot(
        workspace_id,
        all_resources.get(workspace_id),
        official_name=name,
    )
    existing_projects = ws_data.get("projects", {})

    # 增量更新项目
    new_count = 0
    for proj in projects:
        proj_id = proj.get("id", "")
        if proj_id and proj_id not in existing_projects:
            existing_projects[proj_id] = proj
            new_count += 1
        elif proj_id:
            # 更新已有项目的名称（可能有变化）
            existing_projects[proj_id].update(proj)
    
    ws_data["projects"] = existing_projects
    ws_data["updated_at"] = time.time()
    all_resources[workspace_id] = ws_data
    _save_json_file(RESOURCES_FILE, all_resources)
    
    return new_count


def update_workspace_compute_groups(workspace_id: str, compute_groups: List[Dict[str, Any]], name: str = "") -> int:
    """
    增量更新工作空间的计算组列表
    
    Args:
        workspace_id: 工作空间 ID
        compute_groups: 计算组列表 [{"id": ..., "name": ..., "gpu_type": ...}, ...]
        name: 工作空间名称（可选）
        
    Returns:
        新增的计算组数量
    """
    import time

    all_resources = _load_raw_resources()
    ws_data = _prepare_workspace_snapshot(
        workspace_id,
        all_resources.get(workspace_id),
        official_name=name,
    )
    existing_groups = ws_data.get("compute_groups", {})

    # 增量更新计算组
    new_count = 0
    for group in compute_groups:
        group_id = group.get("id", "")
        if group_id and group_id not in existing_groups:
            existing_groups[group_id] = group
            new_count += 1
        elif group_id:
            # 更新已有计算组的信息（可能有变化）
            existing_groups[group_id].update(group)
    
    ws_data["compute_groups"] = existing_groups
    ws_data["updated_at"] = time.time()
    all_resources[workspace_id] = ws_data
    _save_json_file(RESOURCES_FILE, all_resources)
    
    return new_count
