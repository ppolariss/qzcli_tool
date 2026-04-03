"""
资源发现与可用性相关命令。
"""

from typing import Any, Dict, List, Optional

from .api import QzAPIError, get_api
from .config import (
    get_cookie,
    get_workspace_resources,
    list_cached_workspaces,
    load_all_resources,
    save_resources,
    set_workspace_name,
)
from .display import get_display
from .plain_table import format_percent, render_plain_table
from .resource_resolution import (
    ResourceResolutionError,
    resolve_cached_resource_ref,
    resolve_workspace_ref,
)

try:
    from rich.table import Table
    from rich import box

    RICH_TABLE_AVAILABLE = True
except ImportError:
    RICH_TABLE_AVAILABLE = False
    Table = None  # type: ignore
    box = None  # type: ignore


def _cache_workspace_resources(
    api, workspace_id: str, cookie: str, workspace_name: str = ""
) -> Dict[str, int]:
    """从 API 拉取并缓存单个工作空间的资源信息。"""
    result = api.list_jobs_with_cookie(workspace_id, cookie, page_size=200)
    jobs = result.get("jobs", [])
    resources = api.extract_resources_from_jobs(jobs)

    try:
        cluster_info = api.get_cluster_basic_info(workspace_id, cookie)
        compute_groups_from_api = []

        for cg in cluster_info.get("compute_groups", []):
            for lcg in cg.get("logic_compute_groups", []):
                lcg_id = lcg.get("logic_compute_group_id", "")
                lcg_name = lcg.get("logic_compute_group_name", "")
                brand = lcg.get("brand", "")
                resource_types = lcg.get("resource_types", [])
                gpu_type = resource_types[0] if resource_types else ""

                if lcg_id:
                    compute_groups_from_api.append(
                        {
                            "id": lcg_id,
                            "name": lcg_name,
                            "gpu_type": brand or gpu_type,
                            "workspace_id": workspace_id,
                        }
                    )

        if compute_groups_from_api:
            resources["compute_groups"] = compute_groups_from_api
    except Exception:
        pass

    save_resources(workspace_id, resources, workspace_name)
    return {
        "projects": len(resources.get("projects", [])),
        "compute_groups": len(resources.get("compute_groups", [])),
    }


def _parse_cpu_thresholds(raw_values: Optional[List[str]]) -> List[Dict[str, float]]:
    default_values = [
        "20,100",
        "40,200",
        "55,300",
        "55,500",
        "100,400",
        "100,1200",
        "120,500",
    ]
    values = raw_values if raw_values else default_values
    thresholds = []

    for raw in values:
        text = str(raw).strip()
        if "," not in text:
            raise ValueError(f"无效阈值 '{text}'，格式应为 cpu,mem")
        cpu_s, mem_s = text.split(",", 1)
        try:
            cpu = float(cpu_s.strip())
            mem = float(mem_s.strip())
        except ValueError as exc:
            raise ValueError(f"无效阈值 '{text}'，cpu/mem 必须是数字") from exc
        thresholds.append({"cpu": cpu, "mem": mem})

    return thresholds


def _resource_free_value(resource: Dict[str, Any]) -> float:
    if not resource:
        return 0.0
    free_value = resource.get("free")
    if free_value is not None:
        try:
            return float(free_value)
        except Exception:
            return 0.0
    try:
        total = float(resource.get("total", resource.get("total_gib", 0)) or 0)
        used = float(resource.get("used", resource.get("used_gib", 0)) or 0)
        return max(total - used, 0.0)
    except Exception:
        return 0.0


def _node_type_name(node: Dict[str, Any]) -> str:
    return (
        node.get("node_type")
        or node.get("type")
        or node.get("node_type_display")
        or node.get("node_kind")
        or "unknown"
    )


def _analyze_cpu_capacity(
    nodes: List[Dict[str, Any]], thresholds: List[Dict[str, float]]
) -> Dict[str, Any]:
    groups: Dict[str, Dict[str, Any]] = {}
    for node in nodes:
        if str(node.get("status", "")).lower() != "ready":
            continue

        node_type = _node_type_name(node)
        cpu_free = _resource_free_value(node.get("cpu", {}))
        mem_free = _resource_free_value(node.get("memory", {}))

        group = groups.setdefault(
            node_type,
            {
                "ready": 0,
                "cpu_free": 0.0,
                "mem_free": 0.0,
                "above": [0] * len(thresholds),
            },
        )
        group["ready"] += 1
        group["cpu_free"] += cpu_free
        group["mem_free"] += mem_free

        for idx, threshold in enumerate(thresholds):
            if cpu_free >= threshold["cpu"] and mem_free >= threshold["mem"]:
                group["above"][idx] += 1

    overall = {
        "ready": 0,
        "cpu_free": 0.0,
        "mem_free": 0.0,
        "above": [0] * len(thresholds),
    }
    for group in groups.values():
        overall["ready"] += group["ready"]
        overall["cpu_free"] += group["cpu_free"]
        overall["mem_free"] += group["mem_free"]
        overall["above"] = [a + b for a, b in zip(overall["above"], group["above"])]

    return {"groups": groups, "overall": overall}


def _collect_nodes_for_compute_group(
    api,
    workspace_id: str,
    cookie: str,
    logic_compute_group_id: str,
    page_size: int = 200,
) -> List[Dict[str, Any]]:
    nodes: List[Dict[str, Any]] = []
    page_num = 1
    while True:
        data = api.list_node_dimension(
            workspace_id,
            cookie,
            logic_compute_group_id=logic_compute_group_id,
            page_num=page_num,
            page_size=page_size,
        )
        page_nodes = data.get("node_dimensions", [])
        if not page_nodes:
            break

        nodes.extend(page_nodes)
        total = data.get("total")
        if total is not None and len(nodes) >= int(total):
            break
        if len(page_nodes) < page_size:
            break
        page_num += 1

    return nodes


def _print_cpu_capacity_table(
    display, title: str, analysis: Dict[str, Any], thresholds: List[Dict[str, float]]
) -> None:
    groups = analysis["groups"]
    overall = analysis["overall"]

    threshold_labels = [f"{t['cpu']:g},{t['mem']:g}" for t in thresholds]
    headers = ["节点类型", "Ready", "CPU空闲", "MEM空闲"] + [
        f">={label}" for label in threshold_labels
    ]

    rows = []
    for node_type in sorted(groups.keys()):
        group = groups[node_type]
        rows.append(
            [
                node_type,
                group["ready"],
                f"{group['cpu_free']:.2f}",
                f"{group['mem_free']:.2f}",
                *group["above"],
            ]
        )

    rows.append(
        [
            "ALL",
            overall["ready"],
            f"{overall['cpu_free']:.2f}",
            f"{overall['mem_free']:.2f}",
            *overall["above"],
        ]
    )

    display.print(f"[bold]{title}[/bold]")
    if RICH_TABLE_AVAILABLE and getattr(display, "console", None):
        table = Table(
            box=box.MINIMAL,
            show_header=True,
            header_style="bold",
            expand=False,
            padding=(0, 1),
        )
        table.add_column("节点类型", style="cyan", overflow="fold")
        table.add_column("Ready", justify="right")
        table.add_column("CPU空闲", justify="right")
        table.add_column("MEM空闲", justify="right")
        for label in threshold_labels:
            table.add_column(f">={label}", justify="right")

        for row in rows:
            table.add_row(*[str(col) for col in row])
        display.console.print(table)
    else:
        aligns = ["left", "right", "right", "right"] + ["right"] * len(threshold_labels)
        table_lines = render_plain_table(
            headers=headers,
            rows=rows,
            aligns=aligns,
            max_widths=[26, 7, 12, 12] + [9] * len(threshold_labels),
        )
        for line in table_lines:
            display.print(line)
    display.print("")


def cmd_workspaces(args):
    """从历史任务中提取工作空间和资源配置（支持本地缓存）"""
    display = get_display()
    api = get_api()

    if args.list:
        cached = list_cached_workspaces()
        if not cached:
            display.print(
                "[dim]暂无已缓存的工作空间，使用 qzcli catalog -w <workspace_id> 添加[/dim]"
            )
            return 0

        display.print(f"\n[bold]已缓存的工作空间 ({len(cached)} 个)[/bold]\n")
        for ws in cached:
            name = ws.get("name") or "[未命名]"
            alias = ws.get("alias", "")
            official_name = ws.get("official_name", "")
            import datetime

            updated = datetime.datetime.fromtimestamp(ws.get("updated_at", 0)).strftime(
                "%Y-%m-%d %H:%M"
            )
            display.print(f"  [bold]{name}[/bold]")
            if alias and official_name and alias != official_name:
                display.print(f"    官方名: {official_name}")
            display.print(f"    ID: [cyan]{ws['id']}[/cyan]")
            display.print(
                f"    资源: {ws['project_count']} 项目, {ws['compute_group_count']} 计算组, {ws['spec_count']} 规格"
            )
            display.print(f"    更新: {updated}")
            display.print("")

        display.print("[dim]使用方法:[/dim]")
        display.print("  qzcli catalog -w <名称或ID>      # 查看资源目录")
        display.print("  qzcli catalog -w <ID> -u         # 更新缓存")
        display.print("  qzcli catalog -w <ID> --name 别名  # 设置名称")
        return 0

    if hasattr(args, "name") and args.name and not args.update:
        workspace_id = args.workspace
        if not workspace_id:
            display.print_error(
                "请指定工作空间 ID: qzcli catalog -w <workspace_id> --name <名称>"
            )
            return 1
        set_workspace_name(workspace_id, args.name)
        display.print_success(f"已设置工作空间名称: {args.name}")
        return 0

    pending_name = args.name if hasattr(args, "name") else None
    workspace_input = args.workspace
    cookie_data = get_cookie()

    if args.update and not workspace_input:
        try:
            cookie_data = api.ensure_cookie()
        except QzAPIError as e:
            display.print_error(str(e))
            return 1
        cookie = cookie_data["cookie"]
        display.print("[dim]正在获取可访问的工作空间列表...[/dim]")

        try:
            workspaces = api.list_workspaces(cookie)
            if not workspaces:
                display.print_warning("未找到可访问的工作空间")
                return 0

            display.print(f"\n[bold]发现 {len(workspaces)} 个可访问的工作空间[/bold]\n")

            for ws in workspaces:
                ws_id = ws.get("id")
                ws_name = ws.get("name", "")
                display.print(f"[dim]正在更新 {ws_name or ws_id}...[/dim]")

                try:
                    stats = _cache_workspace_resources(api, ws_id, cookie, ws_name)
                    projects_count = stats["projects"]
                    cg_count = stats["compute_groups"]
                    display.print(
                        f"  ✓ {ws_name or ws_id}: {projects_count} 项目, {cg_count} 计算组"
                    )
                except Exception as e:
                    display.print_warning(f"  ✗ {ws_name or ws_id}: {e}")

            display.print("")
            display.print_success("工作空间缓存更新完成！")
            display.print(
                "[dim]使用 qzcli catalog --list 查看所有已缓存的工作空间[/dim]"
            )
            return 0

        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新登录: qzcli login")
            else:
                display.print_error(f"获取工作空间列表失败: {e}")
            return 1

    if not workspace_input:
        workspace_id = cookie_data.get("workspace_id", "") if cookie_data else ""
    else:
        try:
            workspace_id, resolved_name = resolve_workspace_ref(workspace_input)
            if workspace_input != workspace_id:
                display.print(
                    f"[dim]匹配到工作空间: {workspace_input} -> {workspace_id} ({resolved_name})[/dim]"
                )
        except ResourceResolutionError as e:
            display.print_error(str(e))
            display.print("[dim]使用 qzcli catalog --list 查看已缓存的工作空间[/dim]")
            return 1

    if not workspace_id:
        display.print_error("请指定工作空间: qzcli catalog -w <名称或ID>")
        display.print("[dim]使用 qzcli catalog --list 查看已缓存的工作空间[/dim]")
        return 1

    cached_resources = get_workspace_resources(workspace_id)
    use_cache = cached_resources and not args.update

    if use_cache:
        import datetime

        updated = datetime.datetime.fromtimestamp(
            cached_resources.get("updated_at", 0)
        ).strftime("%Y-%m-%d %H:%M")
        ws_name = cached_resources.get("name", "")
        title = "资源配置"
        if ws_name:
            title += f" [{ws_name}]"
        title += f" (缓存于 {updated})"

        display.print(f"\n[bold]{title}[/bold]")
        display.print(f"[dim]工作空间: {workspace_id}[/dim]\n")

        projects = list(cached_resources.get("projects", {}).values())
        compute_groups = list(cached_resources.get("compute_groups", {}).values())
        specs = list(cached_resources.get("specs", {}).values())
    else:
        try:
            cookie_data = api.ensure_cookie()
        except QzAPIError as e:
            display.print_error(str(e))
            return 1
        cookie = cookie_data["cookie"]

        try:
            display.print("[dim]正在从历史任务中提取资源配置...[/dim]")
            result = api.list_jobs_with_cookie(workspace_id, cookie, page_size=200)
            jobs = result.get("jobs", [])
            total = result.get("total", 0)

            if not jobs:
                display.print(
                    "[dim]未找到自己的任务，尝试从工作空间任务获取资源信息...[/dim]"
                )

                projects_found = {}
                compute_groups_found = {}
                gpu_types_found = {}

                try:
                    task_data = api.list_task_dimension(
                        workspace_id, cookie, page_size=200
                    )
                    tasks = task_data.get("task_dimensions", [])

                    for task in tasks:
                        proj = task.get("project", {})
                        proj_id = proj.get("id", "")
                        proj_name = proj.get("name", "")
                        if proj_id and proj_id not in projects_found:
                            projects_found[proj_id] = {
                                "id": proj_id,
                                "name": proj_name,
                                "workspace_id": workspace_id,
                            }
                except QzAPIError:
                    pass

                try:
                    node_data = api.list_node_dimension(
                        workspace_id, cookie, page_size=500
                    )
                    nodes = node_data.get("node_dimensions", [])

                    for node in nodes:
                        lcg_info = node.get("logic_compute_group", {})
                        lcg_id = lcg_info.get("id", "")
                        lcg_name = lcg_info.get("name", "")
                        if lcg_id and lcg_id not in compute_groups_found:
                            gpu_info = node.get("gpu_info", {})
                            gpu_type = gpu_info.get("gpu_product_simple", "")
                            compute_groups_found[lcg_id] = {
                                "id": lcg_id,
                                "name": lcg_name,
                                "gpu_type": gpu_type,
                                "workspace_id": workspace_id,
                            }

                        gpu_info = node.get("gpu_info", {})
                        gpu_type = gpu_info.get("gpu_product_simple", "")
                        if gpu_type and gpu_type not in gpu_types_found:
                            gpu_types_found[gpu_type] = {
                                "type": gpu_type,
                                "display": gpu_info.get("gpu_type_display", ""),
                                "memory_gb": gpu_info.get("gpu_memory_size_gb", 0),
                            }
                except QzAPIError:
                    pass

                resources = {
                    "projects": list(projects_found.values()),
                    "compute_groups": list(compute_groups_found.values()),
                    "specs": [],
                }

                ws_name = pending_name or ""
                save_resources(workspace_id, resources, ws_name)
                display.print_success("已添加工作空间到缓存")

                if projects_found:
                    display.print(f"\n[bold]项目 ({len(projects_found)} 个)[/bold]")
                    for proj in projects_found.values():
                        display.print(f"  - {proj['name']}")
                        display.print(f"    [cyan]{proj['id']}[/cyan]")

                if compute_groups_found:
                    display.print(
                        f"\n[bold]计算组 ({len(compute_groups_found)} 个)[/bold]"
                    )
                    for cg in compute_groups_found.values():
                        display.print(f"  - {cg['name']} [{cg['gpu_type']}]")
                        display.print(f"    [cyan]{cg['id']}[/cyan]")

                if gpu_types_found:
                    display.print(
                        f"\n[bold]可用 GPU 类型 ({len(gpu_types_found)} 种)[/bold]"
                    )
                    for gt in gpu_types_found.values():
                        display.print(
                            f"  - {gt['type']} ({gt['display']}, {gt['memory_gb']}GB)"
                        )

                if not projects_found and not compute_groups_found:
                    display.print("[dim]未发现项目或计算组信息[/dim]")

                return 0

            resources = api.extract_resources_from_jobs(jobs)
            ws_name = pending_name or (
                cached_resources.get("name", "") if cached_resources else ""
            )
            save_resources(workspace_id, resources, ws_name)
            display.print_success("资源配置已保存到本地缓存")

            display.print(
                f"\n[bold]资源配置（从 {len(jobs)}/{total} 个任务中提取）[/bold]"
            )
            display.print(f"[dim]工作空间: {workspace_id}[/dim]\n")

            projects = resources.get("projects", [])
            compute_groups = resources.get("compute_groups", [])
            specs = resources.get("specs", [])

        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error(
                    "Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>"
                )
            else:
                display.print_error(f"获取失败: {e}")
            return 1

    if projects:
        display.print(f"[bold]项目 ({len(projects)} 个)[/bold]")
        for proj in projects:
            display.print(f"  - {proj['name']}")
            display.print(f"    [cyan]{proj['id']}[/cyan]")
        display.print("")

    if compute_groups:
        display.print(f"[bold]计算组 ({len(compute_groups)} 个)[/bold]")
        for group in compute_groups:
            gpu_type = group.get("gpu_type", "")
            gpu_display = group.get("gpu_type_display", "")
            display.print(f"  - {group['name']} [{gpu_type}]")
            if gpu_display:
                display.print(f"    [dim]{gpu_display}[/dim]")
            display.print(f"    [cyan]{group['id']}[/cyan]")
        display.print("")

    if specs:
        display.print(f"[bold]GPU 规格 ({len(specs)} 个)[/bold]")
        for spec in specs:
            gpu_type = spec.get("gpu_type", "")
            gpu_count = spec.get("gpu_count", 0)
            cpu_count = spec.get("cpu_count", 0)
            mem_gb = spec.get("memory_gb", 0)
            display.print(
                f"  - {gpu_count}x {gpu_type} + {cpu_count}核CPU + {mem_gb}GB内存"
            )
            display.print(f"    [cyan]{spec['id']}[/cyan]")
        display.print("")

    if args.export:
        display.print("[bold]导出格式（可用于 shell 脚本）:[/bold]")
        display.print(f'WORKSPACE_ID="{workspace_id}"')
        if projects:
            display.print(f'PROJECT_ID="{projects[0]["id"]}"  # {projects[0]["name"]}')
        if compute_groups:
            for group in compute_groups:
                display.print(f'# {group["name"]} [{group.get("gpu_type", "")}]')
                display.print(f'LOGIC_COMPUTE_GROUP_ID="{group["id"]}"')
        if specs:
            for spec in specs:
                display.print(
                    f'# {spec.get("gpu_count", 0)}x {spec.get("gpu_type", "")}'
                )
                display.print(f'SPEC_ID="{spec["id"]}"')

    return 0


def cmd_resources(args):
    return cmd_workspaces(args)


def cmd_avail(args):
    """查询计算组空余节点，帮助决定任务应该提交到哪里"""
    display = get_display()
    api = get_api()

    try:
        cookie_data = api.ensure_cookie()
    except QzAPIError as e:
        display.print_error(str(e))
        return 1
    cookie = cookie_data["cookie"]
    workspace_input = args.workspace

    if not workspace_input:
        all_resources = load_all_resources()
        if not all_resources:
            display.print_error("没有已缓存的工作空间")
            display.print("[dim]请先运行: qzcli catalog -w <workspace_id> -u[/dim]")
            return 1
        workspace_ids = list(all_resources.keys())
    else:
        try:
            workspace_id, resolved_name = resolve_workspace_ref(workspace_input)
            workspace_ids = [workspace_id]
            if workspace_input != workspace_id:
                display.print(
                    f"[dim]匹配到工作空间: {workspace_input} -> {workspace_id} ({resolved_name})[/dim]"
                )
        except ResourceResolutionError as e:
            display.print_error(str(e))
            display.print("[dim]使用 qzcli catalog --list 查看已缓存的工作空间[/dim]")
            return 1
    if workspace_input and workspace_input.startswith("ws-"):
        workspace_ids = [workspace_input]

    required_nodes = args.nodes
    group_filter = args.group
    all_results = []
    cpu_workspace_results = []

    if args.cpu and args.export:
        display.print_warning("--cpu 模式下忽略 --export")
    if args.cpu and args.low_priority:
        display.print_warning("--cpu 模式下忽略 --lp/--low-priority")
    if args.cpu and required_nodes:
        display.print_warning("--cpu 模式下忽略 --nodes")

    cpu_thresholds: List[Dict[str, float]] = []
    if args.cpu:
        try:
            cpu_thresholds = _parse_cpu_thresholds(args.cpu_th)
        except ValueError as e:
            display.print_error(str(e))
            return 1

    from collections import defaultdict

    for workspace_id in workspace_ids:
        cached_resources = get_workspace_resources(workspace_id)
        if not cached_resources:
            display.print_warning(f"未缓存工作空间 {workspace_id} 的资源信息，跳过")
            continue

        compute_groups = cached_resources.get("compute_groups", {})
        specs = cached_resources.get("specs", {})
        ws_name = cached_resources.get("name", "") or workspace_id

        if group_filter:
            if group_filter.startswith("lcg-"):
                if group_filter in compute_groups:
                    compute_groups = {group_filter: compute_groups[group_filter]}
                else:
                    continue
            else:
                try:
                    group_id, _ = resolve_cached_resource_ref(
                        workspace_id, "compute_groups", group_filter
                    )
                except ResourceResolutionError as e:
                    display.print_warning(f"{ws_name}: {e}")
                    continue
                if group_id in compute_groups:
                    compute_groups = {group_id: compute_groups[group_id]}
                else:
                    continue

        if not compute_groups:
            continue

        display.print(
            f"[dim]正在查询 {ws_name} 的 {len(compute_groups)} 个计算组...[/dim]"
        )

        if args.cpu:
            workspace_nodes = []
            for lcg_id in compute_groups.keys():
                try:
                    workspace_nodes.extend(
                        _collect_nodes_for_compute_group(
                            api,
                            workspace_id,
                            cookie,
                            logic_compute_group_id=lcg_id,
                            page_size=max(1, args.cpu_page_size),
                        )
                    )
                except QzAPIError as e:
                    display.print_warning(f"查询 {ws_name} 的计算组 {lcg_id} 失败: {e}")
                    continue

            if not workspace_nodes:
                display.print_warning(f"{ws_name} 未获取到节点数据")
                continue

            analysis = _analyze_cpu_capacity(workspace_nodes, cpu_thresholds)
            cpu_workspace_results.append(
                {
                    "workspace_id": workspace_id,
                    "workspace_name": ws_name,
                    "analysis": analysis,
                }
            )
            continue

        node_low_priority_gpu = defaultdict(int)

        if args.low_priority:
            display.print("[dim]正在获取低优任务数据（这可能较慢）...[/dim]")
            low_priority_threshold = 3

            try:
                tasks = []
                page_num = 1
                while True:
                    task_data = api.list_task_dimension(
                        workspace_id, cookie, page_num=page_num, page_size=200
                    )
                    page_tasks = task_data.get("task_dimensions", [])
                    tasks.extend(page_tasks)
                    if len(tasks) >= task_data.get("total", 0) or not page_tasks:
                        break
                    page_num += 1

                for task in tasks:
                    priority = task.get("priority", 10)
                    if priority <= low_priority_threshold:
                        gpu_total = task.get("gpu", {}).get("total", 0)
                        nodes_occupied = task.get("nodes_occupied", {}).get("nodes", [])
                        gpu_per_node = (
                            gpu_total // len(nodes_occupied) if nodes_occupied else 0
                        )
                        for node_name in nodes_occupied:
                            node_low_priority_gpu[node_name] += (
                                gpu_per_node if len(nodes_occupied) > 1 else gpu_total
                            )
            except QzAPIError:
                pass

        try:
            for lcg_id, lcg_info in compute_groups.items():
                lcg_name = lcg_info.get("name", lcg_id)
                gpu_type = lcg_info.get("gpu_type", "")

                try:
                    data = api.list_node_dimension(
                        workspace_id, cookie, lcg_id, page_size=1000
                    )
                    nodes = data.get("node_dimensions", [])
                    total_nodes = len(nodes)

                    free_nodes = []
                    low_priority_free_nodes = []
                    gpu_free_distribution = {}
                    total_free_gpus = 0
                    total_gpus = 0

                    for node in nodes:
                        node_name = node.get("name", "")
                        node_status = node.get("status", "")
                        cordon_type = node.get("cordon_type", "")
                        gpu_info = node.get("gpu", {})
                        gpu_used = gpu_info.get("used", 0)
                        gpu_total = gpu_info.get("total", 0)

                        if gpu_total == 0:
                            continue

                        is_schedulable = node_status == "Ready" and not cordon_type
                        gpu_free = max(0, gpu_total - gpu_used)
                        total_gpus += gpu_total

                        if is_schedulable:
                            total_free_gpus += gpu_free

                            if gpu_free > 0:
                                gpu_free_distribution[gpu_free] = (
                                    gpu_free_distribution.get(gpu_free, 0) + 1
                                )

                            if gpu_used == 0 and gpu_total > 0:
                                free_nodes.append(
                                    {
                                        "name": node_name,
                                        "gpu_total": gpu_total,
                                    }
                                )

                            low_priority_gpu = node_low_priority_gpu.get(node_name, 0)
                            if low_priority_gpu >= gpu_total and gpu_used > 0:
                                low_priority_free_nodes.append(
                                    {
                                        "name": node_name,
                                        "low_priority_gpu": low_priority_gpu,
                                        "gpu_total": gpu_total,
                                    }
                                )

                    all_results.append(
                        {
                            "workspace_id": workspace_id,
                            "workspace_name": ws_name,
                            "id": lcg_id,
                            "name": lcg_name,
                            "gpu_type": gpu_type,
                            "total_nodes": total_nodes,
                            "free_nodes": len(free_nodes),
                            "free_node_list": free_nodes,
                            "low_priority_free_nodes": len(low_priority_free_nodes),
                            "low_priority_free_node_list": low_priority_free_nodes,
                            "total_gpus": total_gpus,
                            "total_free_gpus": total_free_gpus,
                            "gpu_free_distribution": gpu_free_distribution,
                            "specs": specs,
                        }
                    )
                except QzAPIError as e:
                    display.print_warning(f"查询 {lcg_name} 失败: {e}")
                    continue
        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error(
                    "Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>"
                )
                return 1
            display.print_warning(f"查询 {ws_name} 失败: {e}")
            continue

    if args.cpu:
        if not cpu_workspace_results:
            display.print_error("未能获取任何工作空间的 CPU/MEM 节点数据")
            return 1

        display.print("\n[bold]CPU/MEM 空闲资源汇总[/bold]\n")
        for entry in cpu_workspace_results:
            ws_name = entry["workspace_name"] or entry["workspace_id"]
            _print_cpu_capacity_table(
                display, f"工作空间: {ws_name}", entry["analysis"], cpu_thresholds
            )

        if len(cpu_workspace_results) > 1:
            merged_overall = {
                "ready": 0,
                "cpu_free": 0.0,
                "mem_free": 0.0,
                "above": [0] * len(cpu_thresholds),
            }
            for entry in cpu_workspace_results:
                overall = entry["analysis"]["overall"]
                merged_overall["ready"] += overall["ready"]
                merged_overall["cpu_free"] += overall["cpu_free"]
                merged_overall["mem_free"] += overall["mem_free"]
                merged_overall["above"] = [
                    a + b for a, b in zip(merged_overall["above"], overall["above"])
                ]
            _print_cpu_capacity_table(
                display,
                "总计（所有工作空间）",
                {"groups": {}, "overall": merged_overall},
                cpu_thresholds,
            )
        return 0

    if not all_results:
        display.print_error("未能获取任何计算组的节点信息")
        return 1

    display.print("\n[bold]空余节点汇总[/bold]\n")

    if required_nodes:
        if args.low_priority:
            all_results.sort(
                key=lambda x: (
                    x["free_nodes"] + x.get("low_priority_free_nodes", 0),
                    x["free_nodes"],
                ),
                reverse=True,
            )
            available = [
                r
                for r in all_results
                if r["free_nodes"] + r.get("low_priority_free_nodes", 0)
                >= required_nodes
            ]
        else:
            all_results.sort(key=lambda x: x["free_nodes"], reverse=True)
            available = [r for r in all_results if r["free_nodes"] >= required_nodes]

        if not available:
            if args.low_priority:
                display.print(
                    f"[red]没有计算组有 >= {required_nodes} 个可用节点（空闲+低优空余）[/red]\n"
                )
            else:
                display.print(
                    f"[red]没有计算组有 >= {required_nodes} 个空闲节点[/red]\n"
                )
            display.print("当前各计算组节点情况：")
            for r in all_results:
                if args.low_priority:
                    lp_free = r.get("low_priority_free_nodes", 0)
                    display.print(
                        f"  [{r['workspace_name']}] {r['name']}: {r['free_nodes']} 空节点 + {lp_free} 低优空余 [{r['gpu_type']}]"
                    )
                else:
                    display.print(
                        f"  [{r['workspace_name']}] {r['name']}: {r['free_nodes']} 空节点 [{r['gpu_type']}]"
                    )
            return 1

        display.print(f"需要 {required_nodes} 个节点，以下计算组可用：\n")

        for r in available:
            if args.low_priority:
                lp_free = r.get("low_priority_free_nodes", 0)
                total_avail = r["free_nodes"] + lp_free
                display.print(
                    f"[green]✓[/green] [{r['workspace_name']}] [bold]{r['name']}[/bold]  {r['free_nodes']} 空节点 + {lp_free} 低优空余 = {total_avail} 可用 [{r['gpu_type']}]"
                )
            else:
                display.print(
                    f"[green]✓[/green] [{r['workspace_name']}] [bold]{r['name']}[/bold]  {r['free_nodes']} 空节点 [{r['gpu_type']}]"
                )
            display.print(f"  [cyan]{r['id']}[/cyan]")
            if args.verbose and r.get("free_node_list"):
                node_names = [n["name"] for n in r["free_node_list"]]
                display.print(f"  [dim]空闲节点: {', '.join(node_names)}[/dim]")
            if (
                args.verbose
                and args.low_priority
                and r.get("low_priority_free_node_list")
            ):
                lp_node_names = [n["name"] for n in r["low_priority_free_node_list"]]
                display.print(f"  [dim]低优空余: {', '.join(lp_node_names)}[/dim]")

        if args.export:
            display.print("")
            best = available[0]
            display.print(
                f"# 推荐: [{best['workspace_name']}] {best['name']} ({best['free_nodes']} 空节点)"
            )
            display.print(f'WORKSPACE_ID="{best["workspace_id"]}"')
            display.print(f'LOGIC_COMPUTE_GROUP_ID="{best["id"]}"')
            specs = best.get("specs", {})
            if specs:
                spec = list(specs.values())[0]
                display.print(
                    f'SPEC_ID="{spec["id"]}"  # {spec.get("gpu_count", 0)}x {spec.get("gpu_type", "")}'
                )
    else:
        if args.low_priority:
            sorted_results = sorted(
                all_results,
                key=lambda x: (
                    x["free_nodes"] + x.get("low_priority_free_nodes", 0),
                    x["free_nodes"],
                    x.get("total_free_gpus", 0),
                ),
                reverse=True,
            )
        else:
            sorted_results = sorted(
                all_results,
                key=lambda x: (x["free_nodes"], x.get("total_free_gpus", 0)),
                reverse=True,
            )

        workspace_order: List[str] = []
        workspace_grouped_results: dict[str, List[dict]] = {}
        for r in sorted_results:
            ws_name = r.get("workspace_name", "")
            if ws_name not in workspace_grouped_results:
                workspace_grouped_results[ws_name] = []
                workspace_order.append(ws_name)
            workspace_grouped_results[ws_name].append(r)

        grouped_results: List[dict] = []
        section_break_after_rows: List[int] = []
        row_cursor = 0
        for ws_name in workspace_order:
            ws_rows = workspace_grouped_results[ws_name]
            grouped_results.extend(ws_rows)
            row_cursor += len(ws_rows)
            if row_cursor < len(sorted_results):
                section_break_after_rows.append(row_cursor - 1)

        total_groups = len(sorted_results)
        total_free_nodes = sum(r.get("free_nodes", 0) for r in sorted_results)
        total_nodes = sum(r.get("total_nodes", 0) for r in sorted_results)
        total_free_gpus = sum(r.get("total_free_gpus", 0) for r in sorted_results)
        total_gpus = sum(r.get("total_gpus", 0) for r in sorted_results)
        total_used_gpus = max(0, total_gpus - total_free_gpus)
        total_gpu_util_ratio = format_percent(total_used_gpus, total_gpus)

        display.print(f"[bold]全分区总览 ({total_groups} 个计算组)[/bold]")
        display.print(
            f"[dim]空节点 {total_free_nodes}/{total_nodes} | 空GPU {total_free_gpus}/{total_gpus} | GPU利用率 {total_gpu_util_ratio}[/dim]"
        )

        if RICH_TABLE_AVAILABLE and getattr(display, "console", None):
            table = Table(
                box=box.MINIMAL,
                show_header=True,
                header_style="bold",
                expand=False,
                padding=(0, 1),
            )
            table.add_column("排名", justify="right", style="dim")
            table.add_column("分区", style="cyan", overflow="fold")
            table.add_column("计算组", style="white", overflow="fold")
            table.add_column("空节点", justify="right")
            if args.low_priority:
                table.add_column("低优空余", justify="right")
                table.add_column("可用节点", justify="right")
            table.add_column("总节点", justify="right", style="dim")
            table.add_column("空GPU", justify="right")
            table.add_column("GPU利用率", justify="right")
            table.add_column("GPU类型", style="magenta", no_wrap=True)

            section_break_set = set(section_break_after_rows)
            for idx, r in enumerate(grouped_results, 1):
                free_nodes = r.get("free_nodes", 0)
                low_priority_free = r.get("low_priority_free_nodes", 0)
                total_available = free_nodes + low_priority_free
                total_gpu = r.get("total_gpus", 0)
                total_free_gpu = r.get("total_free_gpus", 0)

                free_nodes_text = (
                    f"[green]{free_nodes}[/green]" if free_nodes > 0 else "[dim]0[/dim]"
                )
                low_priority_text = (
                    f"[yellow]{low_priority_free}[/yellow]"
                    if low_priority_free > 0
                    else "[dim]0[/dim]"
                )
                total_available_text = (
                    f"[green]{total_available}[/green]"
                    if total_available > 0
                    else "[dim]0[/dim]"
                )

                used_gpu = max(0, total_gpu - total_free_gpu)
                gpu_util_text = format_percent(used_gpu, total_gpu)
                if total_gpu > 0:
                    gpu_util_ratio = used_gpu / total_gpu
                    if gpu_util_ratio >= 0.8:
                        gpu_util_text = f"[green]{gpu_util_text}[/green]"
                    elif gpu_util_ratio >= 0.4:
                        gpu_util_text = f"[yellow]{gpu_util_text}[/yellow]"
                    else:
                        gpu_util_text = f"[red]{gpu_util_text}[/red]"
                else:
                    gpu_util_text = "[dim]-[/dim]"

                row = [
                    str(idx),
                    r.get("workspace_name", ""),
                    r.get("name", ""),
                    free_nodes_text,
                ]
                if args.low_priority:
                    row.extend([low_priority_text, total_available_text])
                row.extend(
                    [
                        str(r.get("total_nodes", 0)),
                        f"{total_free_gpu}/{total_gpu}",
                        gpu_util_text,
                        r.get("gpu_type", "") or "-",
                    ]
                )
                table.add_row(*row, end_section=((idx - 1) in section_break_set))

            display.console.print(table)
        else:
            table_rows = []
            for idx, r in enumerate(grouped_results, 1):
                total_gpu = r.get("total_gpus", 0)
                total_free_gpu = r.get("total_free_gpus", 0)
                row = [
                    idx,
                    r.get("workspace_name", ""),
                    r.get("name", ""),
                    r.get("free_nodes", 0),
                ]
                if args.low_priority:
                    low_priority_free = r.get("low_priority_free_nodes", 0)
                    row.extend(
                        [low_priority_free, r.get("free_nodes", 0) + low_priority_free]
                    )
                row.extend(
                    [
                        r.get("total_nodes", 0),
                        f"{total_free_gpu}/{total_gpu}",
                        format_percent(max(0, total_gpu - total_free_gpu), total_gpu),
                        r.get("gpu_type", "") or "-",
                    ]
                )
                table_rows.append(row)

            headers = ["排名", "分区", "计算组", "空节点"]
            aligns = ["right", "left", "left", "right"]
            max_widths = [4, 24, 30, 6]
            if args.low_priority:
                headers.extend(["低优空余", "可用节点"])
                aligns.extend(["right", "right"])
                max_widths.extend([8, 8])
            headers.extend(["总节点", "空GPU", "GPU利用率", "GPU类型"])
            aligns.extend(["right", "right", "right", "left"])
            max_widths.extend([6, 12, 9, 10])

            table_lines = render_plain_table(
                headers=headers,
                rows=table_rows,
                aligns=aligns,
                max_widths=max_widths,
                section_break_after_rows=section_break_after_rows,
            )
            for line in table_lines:
                display.print(line)

        if args.verbose:
            display.print("")
            display.print("[bold]详细分布[/bold]")
            has_detail = False
            for r in grouped_results:
                prefix = f"[{r.get('workspace_name', '')}] {r.get('name', '')}"
                dist = r.get("gpu_free_distribution", {})
                if dist:
                    dist_parts = []
                    for gpu_count in sorted(dist.keys(), reverse=True):
                        node_count = dist[gpu_count]
                        dist_parts.append(f"空{gpu_count}卡×{node_count}")
                    display.print(f"  [dim]{prefix}: {', '.join(dist_parts)}[/dim]")
                    has_detail = True
                if r.get("free_node_list"):
                    node_names = [n["name"] for n in r["free_node_list"]]
                    display.print(
                        f"  [dim]{prefix} 全空节点: {', '.join(node_names)}[/dim]"
                    )
                    has_detail = True
                if args.low_priority and r.get("low_priority_free_node_list"):
                    lp_node_names = [
                        n["name"] for n in r["low_priority_free_node_list"]
                    ]
                    display.print(
                        f"  [dim]{prefix} 低优空余: {', '.join(lp_node_names)}[/dim]"
                    )
                    has_detail = True
            if not has_detail:
                display.print("  [dim]暂无可展示的详细分布[/dim]")
        display.print("")

        if args.export:
            display.print("[bold]导出格式:[/bold]")
            for r in sorted(all_results, key=lambda x: x["free_nodes"], reverse=True):
                if r["free_nodes"] > 0:
                    display.print(
                        f"# [{r['workspace_name']}] {r['name']} ({r['free_nodes']} 空节点)"
                    )
                    display.print(f'WORKSPACE_ID="{r["workspace_id"]}"')
                    display.print(f'LOGIC_COMPUTE_GROUP_ID="{r["id"]}"')

    return 0
