#!/usr/bin/env python3
"""
qzcli - 启智平台任务管理 CLI
"""

import sys
import time
import argparse
from pathlib import Path

from . import __version__
from .config import (
    init_config, CONFIG_DIR,
    save_cookie, get_cookie, clear_cookie,
    get_workspace_resources, load_all_resources,
)
from .api import get_api, QzAPIError
from .store import get_store, JobRecord
from .display import get_display, format_duration, format_time_ago
from .create_commands import (
    cmd_batch as _cmd_batch_impl,
    cmd_create as _cmd_create_impl,
    cmd_create_hpc as _cmd_create_hpc_impl,
    resolve_create_context as _resolve_create_context_impl,
)
from .resource_commands import (
    _cache_workspace_resources,
    cmd_avail as _cmd_avail_impl,
    cmd_resources as _cmd_resources_impl,
    cmd_workspaces as _cmd_workspaces_impl,
)
from .resource_resolution import ResourceResolutionError, resolve_workspace_ref
from .task_dimensions import cmd_task_dimensions as _cmd_task_dimensions_impl


def cmd_init(args):
    """初始化配置"""
    display = get_display()
    
    username = args.username
    password = args.password
    
    if not username:
        username = input("请输入启智平台用户名: ").strip()
    if not password:
        import getpass
        password = getpass.getpass("请输入密码: ")
    
    if not username or not password:
        display.print_error("用户名和密码不能为空")
        return 1
    
    init_config(username, password)
    
    # 测试连接
    display.print("正在验证连接...")
    api = get_api()
    if api.test_connection():
        display.print_success("配置成功！认证信息已保存")
        display.print(f"配置目录: {CONFIG_DIR}")

        # init 后自动完成 cookie 登录并预拉取全部工作空间到本地缓存，
        # 这样后续可以直接使用 qzcli avail。
        try:
            display.print("[dim]正在获取登录 Cookie...[/dim]")
            cookie = api.login_with_cas(username, password)
            save_cookie(cookie)
            display.print_success("Cookie 已保存")

            display.print("[dim]正在获取可访问的工作空间列表...[/dim]")
            workspaces = api.list_workspaces(cookie)
            if not workspaces:
                display.print_warning("未发现可访问的工作空间，已跳过缓存预热")
                return 0

            display.print(f"[dim]正在预热缓存（共 {len(workspaces)} 个工作空间）...[/dim]")
            success_count = 0
            for ws in workspaces:
                ws_id = ws.get("id", "")
                ws_name = ws.get("name", "")
                if not ws_id:
                    continue
                try:
                    stats = _cache_workspace_resources(api, ws_id, cookie, ws_name)
                    success_count += 1
                    display.print(
                        f"  ✓ {ws_name or ws_id}: {stats['projects']} 项目, {stats['compute_groups']} 计算组"
                    )
                except Exception as e:
                    display.print_warning(f"  ✗ {ws_name or ws_id}: {e}")

            if success_count > 0:
                display.print_success("缓存预热完成，后续可直接运行 qzcli avail")
            else:
                display.print_warning("缓存预热未成功，请稍后手动运行 qzcli catalog -u")
        except QzAPIError as e:
            display.print_warning(f"自动登录或缓存预热失败: {e}")
            display.print("[dim]可稍后手动运行: qzcli login && qzcli catalog -u[/dim]")

        return 0
    else:
        display.print_error("认证失败，请检查用户名和密码")
        return 1


def cmd_list_cookie(args):
    """使用 cookie 从 API 获取任务列表"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    try:
        cookie_data = api.ensure_cookie()
    except QzAPIError as e:
        display.print_error(str(e))
        return 1
    
    cookie = cookie_data["cookie"]
    
    # 确定要查询的工作空间列表
    workspace_input = args.workspace
    
    if args.all_ws:
        # 查询所有已缓存的工作空间
        all_resources = load_all_resources()
        if not all_resources:
            display.print_error("没有已缓存的工作空间")
            display.print("[dim]请先运行: qzcli catalog -w <workspace_id> -u[/dim]")
            return 1
        workspace_ids = [(ws_id, data.get("name", "")) for ws_id, data in all_resources.items()]
    elif workspace_input:
        # 指定的工作空间
        try:
            workspace_id, ws_name = resolve_workspace_ref(workspace_input)
        except ResourceResolutionError as e:
            display.print_error(str(e))
            display.print("[dim]使用 qzcli catalog --list 查看已缓存的工作空间[/dim]")
            return 1
        workspace_ids = [(workspace_id, ws_name)]
    else:
        # 使用默认工作空间
        default_ws = cookie_data.get("workspace_id", "")
        if not default_ws:
            display.print_error("请指定工作空间: qzcli ls -c -w <名称或ID>")
            display.print("[dim]或使用 --all-ws 查询所有已缓存的工作空间[/dim]")
            return 1
        ws_resources = get_workspace_resources(default_ws)
        ws_name = ws_resources.get("name", "") if ws_resources else ""
        workspace_ids = [(default_ws, ws_name)]
    
    all_jobs = []
    
    for workspace_id, ws_name in workspace_ids:
        try:
            if len(workspace_ids) > 1:
                display.print(f"[dim]正在获取 {ws_name or workspace_id} 的任务...[/dim]")
            else:
                display.print(f"[dim]正在从 API 获取任务列表...[/dim]")
            
            result = api.list_jobs_with_cookie(
                workspace_id, 
                cookie, 
                page_size=args.limit * 2 if args.running else args.limit
            )
            
            jobs_data = result.get("jobs", [])
            
            # 转换为 JobRecord 格式
            for job_data in jobs_data:
                job = JobRecord.from_api_response(job_data, source="api_cookie")
                # 添加工作空间名称
                if ws_name:
                    job.metadata["workspace_name"] = ws_name
                all_jobs.append(job)
                
        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file>")
                return 1
            display.print_warning(f"获取 {ws_name or workspace_id} 失败: {e}")
            continue
    
    if not all_jobs:
        display.print("[dim]暂无任务[/dim]")
        return 0
    
    # 按创建时间排序
    all_jobs.sort(key=lambda x: x.created_at or "", reverse=True)
    
    # 过滤状态
    if args.status:
        all_jobs = [j for j in all_jobs if args.status.lower() in j.status.lower()]
    
    # 过滤运行中的任务
    if args.running:
        active_statuses = {"job_running", "job_queuing", "job_pending", "running", "queuing", "pending"}
        all_jobs = [
            j for j in all_jobs
            if j.status.lower() in active_statuses or "running" in j.status.lower() or "queue" in j.status.lower()
        ]
    
    # 限制数量
    all_jobs = all_jobs[:args.limit]
    
    if not all_jobs:
        display.print("[dim]暂无符合条件的任务[/dim]")
        return 0
    
    # 显示标题
    if len(workspace_ids) == 1:
        ws_name = workspace_ids[0][1]
        if ws_name:
            display.print(f"\n[bold]工作空间: {ws_name}[/bold]\n")
    
    # 复用现有显示函数
    if args.wide and not args.compact:
        display.print_jobs_wide(all_jobs)
    else:
        display.print_jobs_table(all_jobs, show_command=args.verbose, show_url=args.url)
    
    return 0


def cmd_list(args):
    """列出任务"""
    # Cookie 模式：从 API 获取任务
    if args.cookie:
        return cmd_list_cookie(args)
    
    display = get_display()
    store = get_store()
    api = get_api()
    
    # 获取本地存储的任务
    # 如果使用 --running，先获取更多任务再过滤
    fetch_limit = args.limit * 3 if args.running else args.limit
    jobs = store.list(limit=fetch_limit, status=args.status)
    
    if not jobs:
        display.print("[dim]暂无任务记录，使用 qzcli import 导入或 qzcli track 添加任务[/dim]")
        return 0
    
    # 更新任务状态
    if not args.no_refresh:
        display.print("[dim]正在更新任务状态...[/dim]")
        
        # 只更新非终态任务
        job_ids_to_update = [
            j.job_id for j in jobs
            if j.status not in ("job_succeeded", "job_failed", "job_stopped")
        ]
        
        if job_ids_to_update:
            try:
                results = api.get_jobs_detail(job_ids_to_update)
                for job_id, data in results.items():
                    if "error" not in data:
                        store.update_from_api(job_id, data)
            except QzAPIError as e:
                display.print_warning(f"部分任务状态更新失败: {e}")
        
        # 重新获取更新后的列表
        jobs = store.list(limit=fetch_limit, status=args.status)
    
    # 过滤：只显示运行中/排队中的任务
    if args.running:
        active_statuses = {"job_running", "job_queuing", "job_pending", "running", "queuing", "pending"}
        jobs = [
            j for j in jobs
            if j.status.lower() in active_statuses or "running" in j.status.lower() or "queue" in j.status.lower()
        ]
        # 应用 limit
        jobs = jobs[:args.limit]
        
        if not jobs:
            display.print("[dim]暂无运行中的任务[/dim]")
            return 0
    
    if args.wide and not args.compact:
        display.print_jobs_wide(jobs)
    else:
        display.print_jobs_table(jobs, show_command=args.verbose, show_url=args.url)
    return 0


def cmd_status(args):
    """查看任务状态"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    job_id = args.job_id
    
    # 从 API 获取最新状态
    try:
        api_data = api.get_job_detail(job_id)
        job = store.update_from_api(job_id, api_data)
        display.print_job_detail(job, api_data)
        
        if args.json:
            import json
            print(json.dumps(api_data, indent=2, ensure_ascii=False))
        
        return 0
    except QzAPIError as e:
        display.print_error(f"查询失败: {e}")
        return 1


def cmd_stop(args):
    """停止任务"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    job_id = args.job_id
    
    # 确认
    if not args.yes:
        confirm = input(f"确定要停止任务 {job_id}? [y/N] ").strip().lower()
        if confirm != "y":
            display.print("已取消")
            return 0
    
    try:
        if api.stop_job(job_id):
            display.print_success(f"任务 {job_id} 已停止")
            # 更新本地状态
            store.update(job_id, status="job_stopped")
            return 0
        else:
            display.print_error("停止任务失败")
            return 1
    except QzAPIError as e:
        display.print_error(f"停止任务失败: {e}")
        return 1


def cmd_watch(args):
    """实时监控任务状态"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    interval = args.interval
    
    display.print(f"[bold]实时监控模式[/bold] (每 {interval} 秒刷新，按 Ctrl+C 退出)")
    display.print("")
    
    try:
        while True:
            # 获取所有非终态任务
            jobs = store.list()
            active_jobs = [
                j for j in jobs
                if j.status not in ("job_succeeded", "job_failed", "job_stopped")
            ]
            
            # 更新状态
            if active_jobs:
                job_ids = [j.job_id for j in active_jobs]
                try:
                    results = api.get_jobs_detail(job_ids)
                    for job_id, data in results.items():
                        if "error" not in data:
                            store.update_from_api(job_id, data)
                except QzAPIError:
                    pass
            
            # 清屏并显示
            print("\033[2J\033[H", end="")  # 清屏
            
            jobs = store.list(limit=args.limit)
            display.print_jobs_table(
                jobs,
                title=f"启智平台任务监控 (每 {interval}s 刷新)"
            )
            
            # 检查是否还有活跃任务
            active_count = sum(
                1 for j in jobs
                if j.status not in ("job_succeeded", "job_failed", "job_stopped")
            )
            
            if active_count == 0 and not args.keep_alive:
                display.print("\n[green]所有任务已完成[/green]")
                break
            
            time.sleep(interval)
    
    except KeyboardInterrupt:
        display.print("\n[dim]监控已停止[/dim]")
    
    return 0


def cmd_track(args):
    """追踪任务（供脚本调用）"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    job_id = args.job_id
    
    # 尝试从 API 获取详情
    try:
        api_data = api.get_job_detail(job_id)
        job = JobRecord.from_api_response(api_data, source=args.source or "")
    except QzAPIError:
        # API 失败时创建最小记录
        job = JobRecord(
            job_id=job_id,
            name=args.name or "",
            source=args.source or "",
            workspace_id=args.workspace or "",
        )
    
    # 更新元数据
    if args.name:
        job.name = args.name
    if args.source:
        job.source = args.source
    if args.workspace:
        job.workspace_id = args.workspace
    
    store.add(job)
    
    if not args.quiet:
        display.print_success(f"已追踪任务: {job_id}")
    
    return 0


def cmd_import(args):
    """从文件导入任务"""
    display = get_display()
    store = get_store()
    api = get_api()
    
    filepath = Path(args.file)
    if not filepath.exists():
        display.print_error(f"文件不存在: {filepath}")
        return 1
    
    count = store.import_from_file(filepath, source=args.source or filepath.name)
    display.print_success(f"已导入 {count} 个任务")
    
    # 可选：更新导入任务的状态
    if args.refresh and count > 0:
        display.print("正在更新任务状态...")
        jobs = store.list()
        job_ids = [j.job_id for j in jobs if not j.status or j.status == "unknown"]
        
        if job_ids:
            try:
                results = api.get_jobs_detail(job_ids[:50])  # 最多更新 50 个
                updated = 0
                for job_id, data in results.items():
                    if "error" not in data:
                        store.update_from_api(job_id, data)
                        updated += 1
                display.print_success(f"已更新 {updated} 个任务状态")
            except QzAPIError as e:
                display.print_warning(f"状态更新失败: {e}")
    
    return 0


def cmd_remove(args):
    """删除任务记录"""
    display = get_display()
    store = get_store()
    
    job_id = args.job_id
    
    if not args.yes:
        confirm = input(f"确定要删除任务记录 {job_id}? [y/N] ").strip().lower()
        if confirm != "y":
            display.print("已取消")
            return 0
    
    if store.remove(job_id):
        display.print_success(f"已删除任务记录: {job_id}")
        return 0
    else:
        display.print_error(f"任务不存在: {job_id}")
        return 1


def cmd_clear(args):
    """清空所有任务记录"""
    display = get_display()
    store = get_store()
    
    count = store.count()
    
    if count == 0:
        display.print("暂无任务记录")
        return 0
    
    if not args.yes:
        confirm = input(f"确定要清空所有 {count} 个任务记录? [y/N] ").strip().lower()
        if confirm != "y":
            display.print("已取消")
            return 0
    
    store.clear()
    display.print_success(f"已清空 {count} 个任务记录")
    return 0


def cmd_task_dimensions(args):
    """查询 cluster_metric task dimensions。"""
    return _cmd_task_dimensions_impl(args)


def cmd_cookie(args):
    """设置浏览器 cookie"""
    display = get_display()
    
    if args.clear:
        clear_cookie()
        display.print_success("已清除 cookie")
        return 0
    
    if args.show:
        cookie_data = get_cookie()
        if cookie_data:
            display.print(f"Workspace: {cookie_data.get('workspace_id', 'N/A')}")
            display.print(f"Cookie: {cookie_data.get('cookie', '')[:80]}...")
        else:
            display.print("[dim]未设置 cookie[/dim]")
        return 0
    
    cookie = args.cookie
    workspace_id = args.workspace or ""
    
    # 支持从文件读取 cookie
    if args.file:
        filepath = Path(args.file)
        if not filepath.exists():
            display.print_error(f"文件不存在: {filepath}")
            return 1
        with open(filepath, "r") as f:
            lines = f.readlines()
            # 取最后一个非空行作为 cookie
            for line in reversed(lines):
                line = line.strip()
                if line and not line.startswith("#") and line != "cookie":
                    cookie = line
                    break
        if not cookie:
            display.print_error("文件中未找到有效的 cookie")
            return 1
        display.print(f"[dim]从文件读取 cookie: {filepath}[/dim]")
    
    if not cookie:
        display.print("请输入浏览器 cookie（从 F12 Network 中复制）:")
        display.print("[dim]提示: 在 qz.sii.edu.cn 页面按 F12 -> Console -> 输入 document.cookie[/dim]")
        cookie = input().strip()
    
    if not cookie:
        display.print_error("cookie 不能为空")
        return 1
    
    # 测试 cookie 是否有效（使用 /openapi/v1/train_job/list 端点）
    if not args.no_test and workspace_id:
        display.print("正在验证 cookie...")
        api = get_api()
        try:
            result = api.list_jobs_with_cookie(workspace_id, cookie, page_size=1)
            total = result.get("total", 0)
            display.print_success(f"Cookie 有效！工作空间内有 {total} 个任务")
        except QzAPIError as e:
            display.print_error(f"Cookie 无效: {e}")
            return 1
    
    save_cookie(cookie, workspace_id)
    display.print_success("Cookie 已保存")
    return 0


def cmd_workspaces(args):
    """从历史任务中提取工作空间和资源配置（支持本地缓存）"""
    return _cmd_workspaces_impl(args)


def cmd_resources(args):
    """列出工作空间内可用的计算资源（cmd_workspaces 的别名）"""
    return _cmd_resources_impl(args)


def cmd_avail(args):
    """查询计算组空余节点，帮助决定任务应该提交到哪里"""
    return _cmd_avail_impl(args)


def cmd_usage(args):
    """统计工作空间的 GPU 使用分布"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    try:
        cookie_data = api.ensure_cookie()
    except QzAPIError as e:
        display.print_error(str(e))
        return 1
    
    cookie = cookie_data["cookie"]
    
    # 解析 workspace 参数
    workspace_input = args.workspace
    
    if not workspace_input:
        # 查询所有已缓存的工作空间
        all_resources = load_all_resources()
        if not all_resources:
            display.print_error("没有已缓存的工作空间")
            display.print("[dim]请先运行: qzcli catalog -u[/dim]")
            return 1
        workspace_ids = [(ws_id, data.get("name", "")) for ws_id, data in all_resources.items()]
    else:
        try:
            workspace_id, ws_name = resolve_workspace_ref(workspace_input)
            workspace_ids = [(workspace_id, ws_name)]
        except ResourceResolutionError as e:
            display.print_error(str(e))
            return 1
    if workspace_input and workspace_input.startswith("ws-"):
        ws_resources = get_workspace_resources(workspace_input)
        ws_name = ws_resources.get("name", "") if ws_resources else ""
        workspace_ids = [(workspace_input, ws_name)]
    
    from collections import defaultdict
    
    all_stats = []
    
    for workspace_id, ws_name in workspace_ids:
        display.print(f"[dim]正在查询 {ws_name or workspace_id}...[/dim]")
        
        try:
            # 分页获取所有任务
            tasks = []
            page_num = 1
            page_size = 200
            while True:
                data = api.list_task_dimension(workspace_id, cookie, page_num=page_num, page_size=page_size)
                page_tasks = data.get("task_dimensions", [])
                total_count = data.get("total", 0)
                tasks.extend(page_tasks)
                
                if len(tasks) >= total_count or not page_tasks:
                    break
                page_num += 1
            
            if not tasks:
                continue
            
            # 统计 GPU 分布
            gpu_distribution = defaultdict(int)  # gpu_count -> task_count
            user_gpu = defaultdict(int)  # user -> total_gpu
            project_gpu = defaultdict(int)  # project -> total_gpu
            type_stats = defaultdict(lambda: {"count": 0, "gpu": 0})  # type -> {count, gpu}
            priority_stats = defaultdict(lambda: {"count": 0, "gpu": 0})  # priority -> {count, gpu}
            total_gpu = 0
            total_tasks = len(tasks)
            
            # 任务类型中文映射
            type_names = {
                "distributed_training": "分布式训练",
                "interactive_modeling": "交互式建模",
                "inference_serving_customize": "推理服务",
                "inference_serving": "推理服务",
                "training": "训练",
            }
            
            for task in tasks:
                gpu_info = task.get("gpu", {})
                gpu_total = gpu_info.get("total", 0)
                user_name = task.get("user", {}).get("name", "未知")
                project_info = task.get("project", {})
                project_name = project_info.get("name", "未知")
                task_type = task.get("type", "unknown")
                priority = task.get("priority", 0)
                
                gpu_distribution[gpu_total] += 1
                user_gpu[user_name] += gpu_total
                project_gpu[project_name] += gpu_total
                type_stats[task_type]["count"] += 1
                type_stats[task_type]["gpu"] += gpu_total
                priority_stats[priority]["count"] += 1
                priority_stats[priority]["gpu"] += gpu_total
                total_gpu += gpu_total
            
            all_stats.append({
                "workspace_id": workspace_id,
                "workspace_name": ws_name,
                "total_tasks": total_tasks,
                "total_gpu": total_gpu,
                "gpu_distribution": dict(gpu_distribution),
                "user_gpu": dict(user_gpu),
                "project_gpu": dict(project_gpu),
                "type_stats": dict(type_stats),
                "type_names": type_names,
                "priority_stats": dict(priority_stats),
            })
            
        except QzAPIError as e:
            if "401" in str(e) or "过期" in str(e):
                display.print_error("Cookie 已过期，请重新设置: qzcli login")
                return 1
            display.print_warning(f"查询 {ws_name or workspace_id} 失败: {e}")
            continue
    
    if not all_stats:
        display.print("[dim]暂无运行中的任务[/dim]")
        return 0
    
    # 显示结果
    for stats in all_stats:
        ws_name = stats["workspace_name"] or stats["workspace_id"]
        display.print(f"\n[bold]{ws_name}[/bold]")
        display.print(f"运行中: {stats['total_tasks']} 个任务, 共 {stats['total_gpu']} GPU\n")
        
        # GPU 卡数分布
        display.print("[bold]GPU 卡数分布:[/bold]")
        gpu_dist = stats["gpu_distribution"]
        for gpu_count in sorted(gpu_dist.keys()):
            task_count = gpu_dist[gpu_count]
            bar = "█" * min(task_count, 30)
            display.print(f"  {gpu_count:>3} GPU: {task_count:>3} 任务 {bar}")
        
        # 按用户统计（可选）
        if args.by_user:
            display.print("\n[bold]按用户统计:[/bold]")
            user_gpu = stats["user_gpu"]
            for user, gpu in sorted(user_gpu.items(), key=lambda x: -x[1]):
                display.print(f"  {user:<12} {gpu:>4} GPU")
        
        # 按项目统计（可选）
        if args.by_project:
            display.print("\n[bold]按项目统计:[/bold]")
            project_gpu = stats["project_gpu"]
            for project, gpu in sorted(project_gpu.items(), key=lambda x: -x[1]):
                proj_display = project[:25] if len(project) > 25 else project
                display.print(f"  {proj_display:<27} {gpu:>4} GPU")
        
        # 按任务类型统计（可选）
        if args.by_type:
            display.print("\n[bold]按任务类型统计:[/bold]")
            type_stats = stats["type_stats"]
            type_names = stats["type_names"]
            for task_type, info in sorted(type_stats.items(), key=lambda x: -x[1]["gpu"]):
                type_display = type_names.get(task_type, task_type)
                display.print(f"  {type_display:<20} {info['count']:>4} 任务  {info['gpu']:>5} GPU")
        
        # 按优先级统计（可选）
        if args.by_priority:
            display.print("\n[bold]按优先级统计:[/bold]")
            priority_stats = stats["priority_stats"]
            for priority, info in sorted(priority_stats.items(), key=lambda x: -x[0]):
                display.print(f"  优先级 {priority:<10} {info['count']:>4} 任务  {info['gpu']:>5} GPU")
        
        display.print("")
    
    # 汇总
    if len(all_stats) > 1:
        total_tasks = sum(s["total_tasks"] for s in all_stats)
        total_gpu = sum(s["total_gpu"] for s in all_stats)
        display.print(f"[bold]总计: {total_tasks} 个任务, {total_gpu} GPU[/bold]")
    
    return 0


def cmd_workspace(args):
    """查看工作空间内运行任务。"""
    display = get_display()
    api = get_api()
    
    # 获取 cookie
    try:
        cookie_data = api.ensure_cookie()
    except QzAPIError as e:
        display.print_error(str(e))
        return 1
    
    cookie = cookie_data["cookie"]
    workspace_arg = args.workspace
    if workspace_arg:
        try:
            workspace_id, _ = resolve_workspace_ref(workspace_arg)
        except ResourceResolutionError as e:
            display.print_error(str(e))
            return 1
    else:
        workspace_id = cookie_data.get("workspace_id", "")
    
    # 如果没有指定 workspace，列出可用的 workspace 供选择
    if not workspace_id:
        display.print("[yellow]未设置默认工作空间，正在获取可用列表...[/yellow]\n")
        try:
            workspaces = api.list_workspaces(cookie)
            if workspaces:
                display.print("[bold]请选择一个工作空间:[/bold]\n")
                for idx, ws in enumerate(workspaces, 1):
                    ws_id = ws.get("id", "")
                    ws_name = ws.get("name", "未命名")
                    display.print(f"  [{idx}] {ws_name}")
                    display.print(f"      [dim]{ws_id}[/dim]")
                display.print("")
                display.print("[dim]使用方法:[/dim]")
                display.print("  qzcli ws -w <工作空间名称或ID>")
                display.print("  qzcli cookie -w <workspace_id>  # 设置默认")
            else:
                display.print_error("未找到可访问的工作空间")
        except QzAPIError as e:
            display.print_error(f"获取工作空间列表失败: {e}")
        return 1
    
    # 项目过滤
    project_filter = None if args.all else args.project
    
    try:
        display.print("[dim]正在获取工作空间任务...[/dim]")
        result = api.list_workspace_tasks(
            workspace_id, 
            cookie,
            page_num=args.page,
            page_size=args.size,
            project_filter=project_filter,
        )
        
        tasks = result.get("task_dimensions", [])
        total = result.get("total", 0)
        
        if not tasks:
            if project_filter:
                display.print(f"[dim]项目 '{project_filter}' 暂无运行中的任务[/dim]")
            else:
                display.print("工作空间内暂无运行中的任务")
            return 0
        
        # 统计 GPU 使用
        total_gpu = sum(t.get("gpu", {}).get("total", 0) for t in tasks)
        avg_gpu_usage = sum(t.get("gpu", {}).get("usage_rate", 0) for t in tasks) / len(tasks) * 100 if tasks else 0
        
        title = f"工作空间任务概览"
        if project_filter:
            title += f" [{project_filter}]"
        title += f" (显示 {len(tasks)}/{total} 个, {total_gpu} GPU, 平均利用率 {avg_gpu_usage:.1f}%)"
        
        display.print(f"\n[bold]{title}[/bold]\n")
        
        # 同步到本地任务列表
        synced_count = 0
        if args.sync:
            store = get_store()
            for task in tasks:
                job_id = task.get("id", "")
                if job_id and not store.get(job_id):
                    # 创建简化的 JobRecord
                    from .store import JobRecord
                    job = JobRecord(
                        job_id=job_id,
                        name=task.get("name", ""),
                        status=task.get("status", "UNKNOWN").lower(),
                        source="workspace_sync",
                        workspace_id=workspace_id,
                        project_name=task.get("project", {}).get("name", ""),
                    )
                    store.add(job)
                    synced_count += 1
            if synced_count > 0:
                display.print_success(f"已同步 {synced_count} 个新任务到本地")
        
        for idx, task in enumerate(tasks, 1):
            name = task.get("name", "")
            status = task.get("status", "UNKNOWN")
            gpu_total = task.get("gpu", {}).get("total", 0)
            gpu_usage = task.get("gpu", {}).get("usage_rate", 0) * 100
            cpu_usage = task.get("cpu", {}).get("usage_rate", 0) * 100
            mem_usage = task.get("memory", {}).get("usage_rate", 0) * 100
            nodes_info = task.get("nodes_occupied", {})
            nodes_count = nodes_info.get("count", 0)
            nodes_list = nodes_info.get("nodes", [])
            user_name = task.get("user", {}).get("name", "")
            project_name = task.get("project", {}).get("name", "")
            running_time = format_duration(task.get("running_time_ms", ""))
            job_id = task.get("id", "")
            
            # 状态颜色
            if status == "RUNNING":
                status_icon = "[cyan]●[/cyan]"
            elif status == "QUEUING":
                status_icon = "[yellow]◌[/yellow]"
            else:
                status_icon = "[dim]?[/dim]"
            
            # GPU 使用率颜色
            if gpu_usage >= 80:
                gpu_color = "green"
            elif gpu_usage >= 50:
                gpu_color = "yellow"
            else:
                gpu_color = "red"
            
            display.print(f"[bold][{idx:2d}][/bold] {status_icon} {name}")
            display.print(f"     [{gpu_color}]{gpu_total} GPU ({gpu_usage:.0f}%)[/{gpu_color}] | CPU {cpu_usage:.0f}% | MEM {mem_usage:.0f}% | {running_time} | {user_name}")
            display.print(f"     [dim]{project_name} | {nodes_count} 节点: {', '.join(nodes_list[:3])}{'...' if len(nodes_list) > 3 else ''}[/dim]")
            display.print(f"     [dim]{job_id}[/dim]")
            display.print("")
        
        return 0
        
    except QzAPIError as e:
        if "401" in str(e) or "过期" in str(e):
            display.print_error("Cookie 已过期，请重新设置: qzcli cookie -f <cookie_file> -w <workspace_id>")
        else:
            display.print_error(f"获取失败: {e}")
        return 1


def _resolve_create_context(args, display):
    return _resolve_create_context_impl(args, display)


def cmd_create(args):
    return _cmd_create_impl(args)


def cmd_create_hpc(args):
    return _cmd_create_hpc_impl(args)


def cmd_batch(args):
    return _cmd_batch_impl(args)


def cmd_login(args):
    """通过 CAS 登录获取 cookie"""
    import getpass
    
    display = get_display()
    api = get_api()
    
    # 获取用户名
    username = args.username
    if not username:
        try:
            username = input("学工号: ").strip()
        except (EOFError, KeyboardInterrupt):
            display.print("\n[dim]已取消[/dim]")
            return 1
    
    if not username:
        display.print_error("用户名不能为空")
        return 1
    
    # 获取密码
    password = args.password
    if getattr(args, 'password_stdin', False):
        try:
            password = sys.stdin.readline().rstrip('\n')
        except (EOFError, KeyboardInterrupt):
            display.print_error("未从 stdin 读取到密码")
            return 1
    if not password:
        try:
            password = getpass.getpass("密码: ")
        except (EOFError, KeyboardInterrupt):
            display.print("\n[dim]已取消[/dim]")
            return 1
    
    if not password:
        display.print_error("密码不能为空")
        return 1
    
    display.print("[dim]正在登录...[/dim]")
    
    try:
        cookie = api.login_with_cas(username, password)
        
        # 保存 cookie
        save_cookie(cookie, workspace_id=args.workspace)
        
        display.print_success("登录成功！Cookie 已保存")
        
        # 显示 cookie 前几个字符
        cookie_preview = cookie[:50] + "..." if len(cookie) > 50 else cookie
        display.print(f"[dim]Cookie: {cookie_preview}[/dim]")
        
        if args.workspace:
            display.print(f"[dim]默认工作空间: {args.workspace}[/dim]")
        
        return 0
        
    except QzAPIError as e:
        display.print_error(f"登录失败: {e}")
        return 1


def main():
    """主入口"""
    parser = argparse.ArgumentParser(
        prog="qzcli",
        description="启智平台任务管理 CLI 工具",
    )
    parser.add_argument(
        "--version", "-V",
        action="version",
        version=f"qzcli {__version__}"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="子命令")
    
    # init 命令
    init_parser = subparsers.add_parser("init", help="初始化配置")
    init_parser.add_argument("--username", "-u", help="用户名")
    init_parser.add_argument("--password", "-p", help="密码")
    
    # list 命令
    list_parser = subparsers.add_parser("list", aliases=["ls"], help="列出任务")
    list_parser.add_argument("--limit", "-n", type=int, default=20, help="显示数量限制")
    list_parser.add_argument("--status", "-s", help="按状态过滤")
    list_parser.add_argument("--running", "-r", action="store_true", help="只显示运行中/排队中的任务")
    list_parser.add_argument("--no-refresh", action="store_true", help="不更新状态")
    list_parser.add_argument("--verbose", "-v", action="store_true", help="显示详细信息")
    list_parser.add_argument("--url", "-u", action="store_true", default=True, help="显示任务链接（默认开启）")
    list_parser.add_argument("--wide", action="store_true", default=True, help="宽格式显示（默认开启）")
    list_parser.add_argument("--compact", action="store_true", help="紧凑表格格式（关闭宽格式）")
    # Cookie 模式参数
    list_parser.add_argument("--cookie", "-c", action="store_true", help="使用 cookie 从 API 获取任务（无需本地 store）")
    list_parser.add_argument("--workspace", "-w", help="工作空间（名称或 ID，cookie 模式）")
    list_parser.add_argument("--all-ws", action="store_true", help="查询所有已缓存的工作空间（cookie 模式）")
    
    # status 命令
    status_parser = subparsers.add_parser("status", aliases=["st"], help="查看任务状态")
    status_parser.add_argument("job_id", help="任务 ID")
    status_parser.add_argument("--json", "-j", action="store_true", help="输出 JSON")
    
    # stop 命令
    stop_parser = subparsers.add_parser("stop", help="停止任务")
    stop_parser.add_argument("job_id", help="任务 ID")
    stop_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")
    
    # watch 命令
    watch_parser = subparsers.add_parser("watch", aliases=["w"], help="实时监控")
    watch_parser.add_argument("--interval", "-i", type=int, default=10, help="刷新间隔（秒）")
    watch_parser.add_argument("--limit", "-n", type=int, default=30, help="显示数量限制")
    watch_parser.add_argument("--keep-alive", "-k", action="store_true", help="所有任务完成后继续监控")
    
    # track 命令（供脚本调用）
    track_parser = subparsers.add_parser("track", help="追踪任务")
    track_parser.add_argument("job_id", help="任务 ID")
    track_parser.add_argument("--name", help="任务名称")
    track_parser.add_argument("--source", help="来源脚本")
    track_parser.add_argument("--workspace", help="工作空间 ID")
    track_parser.add_argument("--quiet", "-q", action="store_true", help="静默模式")
    
    # import 命令
    import_parser = subparsers.add_parser("import", help="从文件导入任务")
    import_parser.add_argument("file", help="包含任务 ID 的文件")
    import_parser.add_argument("--source", help="来源标记")
    import_parser.add_argument("--refresh", "-r", action="store_true", help="导入后更新状态")
    
    # remove 命令
    remove_parser = subparsers.add_parser("remove", aliases=["rm"], help="删除任务记录")
    remove_parser.add_argument("job_id", help="任务 ID")
    remove_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")
    
    # clear 命令
    clear_parser = subparsers.add_parser("clear", help="清空所有任务记录")
    clear_parser.add_argument("--yes", "-y", action="store_true", help="跳过确认")
    
    # cookie 命令
    cookie_parser = subparsers.add_parser("cookie", help="设置浏览器 cookie（用于访问内部 API）")
    cookie_parser.add_argument("cookie", nargs="?", help="浏览器 cookie 字符串")
    cookie_parser.add_argument("--file", "-f", help="从文件读取 cookie")
    cookie_parser.add_argument("--workspace", "-w", help="默认工作空间 ID")
    cookie_parser.add_argument("--show", action="store_true", help="显示当前 cookie")
    cookie_parser.add_argument("--clear", action="store_true", help="清除 cookie")
    cookie_parser.add_argument("--no-test", action="store_true", help="不测试 cookie 有效性")
    
    # login 命令
    login_parser = subparsers.add_parser("login", help="通过 CAS 统一认证登录获取 cookie")
    login_parser.add_argument("--username", "-u", help="学工号")
    login_parser.add_argument("--password", "-p", help="密码（含特殊字符时建议用单引号或 --password-stdin）")
    login_parser.add_argument("--password-stdin", action="store_true", help="从 stdin 读取密码（适合脚本: echo 'pass' | qzcli login -u user --password-stdin）")
    login_parser.add_argument("--workspace", "-w", help="默认工作空间 ID")
    
    # workspace 命令
    workspace_parser = subparsers.add_parser("workspace", aliases=["ws"], help="查看工作空间内运行任务")
    workspace_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    workspace_parser.add_argument("--project", "-p", help="按项目名称过滤（默认不过滤）")
    workspace_parser.add_argument("--all", "-a", action="store_true", help="显示所有项目（兼容旧参数，默认已不过滤）")
    workspace_parser.add_argument("--page", type=int, default=1, help="页码")
    workspace_parser.add_argument("--size", type=int, default=100, help="每页数量（默认 100）")
    workspace_parser.add_argument("--sync", "-s", action="store_true", help="同步到本地任务列表")
    
    # catalog 命令 - 管理工作空间资源目录
    workspaces_parser = subparsers.add_parser("catalog", aliases=["workspaces", "lsws", "res", "resources"], help="查看和刷新工作空间资源目录（项目、计算组、规格）")
    workspaces_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    workspaces_parser.add_argument("--export", "-e", action="store_true", help="输出可用于脚本的环境变量格式")
    workspaces_parser.add_argument("--update", "-u", action="store_true", help="强制从 API 更新缓存")
    workspaces_parser.add_argument("--list", "-l", action="store_true", help="列出所有已缓存的工作空间")
    workspaces_parser.add_argument("--name", help="设置工作空间名称（别名）")
    
    # avail 命令 - 查询空余节点
    avail_parser = subparsers.add_parser("avail", aliases=["av"], help="查询计算组空余节点，帮助决定任务应该提交到哪里")
    avail_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    avail_parser.add_argument("--group", "-g", help="计算组 ID 或名称（可选，不指定则查询所有）")
    avail_parser.add_argument("--nodes", "-n", type=int, help="需要的节点数（推荐模式：找出满足条件的计算组）")
    avail_parser.add_argument("--export", "-e", action="store_true", help="输出可用于脚本的环境变量格式")
    avail_parser.add_argument("--verbose", "-v", action="store_true", help="显示空闲节点名称列表")
    avail_parser.add_argument("--lp", "--low-priority", action="store_true", dest="low_priority", help="计算低优任务占用节点（较慢）")
    avail_parser.add_argument("--cpu", action="store_true", help="按节点类型统计 CPU/MEM 空闲资源")
    avail_parser.add_argument("--cpu-th", action="append", help="CPU/MEM 阈值，格式 cpu,mem；可重复")
    avail_parser.add_argument("--cpu-page-size", type=int, default=200, help="CPU 统计模式节点分页大小（默认 200）")
    
    # usage 命令
    usage_parser = subparsers.add_parser("usage", help="统计工作空间的 GPU 使用分布")
    usage_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    usage_parser.add_argument("--by-user", "-u", action="store_true", help="按用户统计 GPU 使用")
    usage_parser.add_argument("--by-project", "-p", action="store_true", help="按项目统计 GPU 使用")
    usage_parser.add_argument("--by-type", "-t", action="store_true", help="按任务类型统计（训练/建模/部署）")
    usage_parser.add_argument("--by-priority", "-r", action="store_true", help="按优先级统计")

    # tasks 命令 - 直接查看 cluster_metric 任务维度，可启动本地前端
    task_dims_parser = subparsers.add_parser(
        "tasks",
        aliases=["jobs", "blame"],
        help="查看 /api/v1/cluster_metric/list_task_dimension 并启动本地前端",
    )
    task_dims_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称")
    task_dims_parser.add_argument("--project", "-p", help="项目 ID 或名称")
    task_dims_parser.add_argument("--page-size", type=int, default=100, help="后端分页大小（默认 100）")
    task_dims_parser.add_argument("--serve", dest="serve", action="store_true", default=True, help="启动本地前端（默认开启）")
    task_dims_parser.add_argument("--no-serve", dest="serve", action="store_false", help="只输出命令行表格，不启动前端")
    task_dims_parser.add_argument("--host", default="127.0.0.1", help="前端监听地址（默认 127.0.0.1）")
    task_dims_parser.add_argument("--port", type=int, default=8765, help="前端监听端口（默认 8765）")

    # create 命令 - 创建任务
    create_parser = subparsers.add_parser("create", aliases=["create-job"], help="创建并提交任务到启智平台")
    create_parser.add_argument("--name", "-n", required=True, help="任务名称")
    create_parser.add_argument("--command", "-c", dest="cmd_str", required=True, help="执行命令")
    create_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称（从 qzcli catalog 缓存解析）")
    create_parser.add_argument("--project", "-p", help="项目 ID 或名称（仅唯一候选时自动选择）")
    create_parser.add_argument("--compute-group", "-g", dest="compute_group", help="计算组 ID 或名称（仅唯一候选时自动选择）")
    create_parser.add_argument("--spec", "-s", help="资源规格 ID（仅唯一候选时自动选择）")
    create_parser.add_argument("--image", "-i", help="Docker 镜像")
    create_parser.add_argument("--image-type", dest="image_type", default="SOURCE_PRIVATE", help="镜像类型（默认 SOURCE_PRIVATE）")
    create_parser.add_argument("--instances", type=int, default=1, help="实例数量（默认 1）")
    create_parser.add_argument("--shm", type=int, default=1200, help="共享内存 GiB（默认 1200）")
    create_parser.add_argument("--priority", type=int, default=10, help="任务优先级 1-10（默认 10）")
    create_parser.add_argument("--framework", default="pytorch", help="框架类型（默认 pytorch）")
    create_parser.add_argument(
        "--auto-fault-tolerance",
        "--auto_fault_tolerance",
        dest="auto_fault_tolerance",
        action="store_true",
        help="启用自动容错",
    )
    create_parser.add_argument(
        "--fault-tolerance-max-retry",
        "--fault_tolerance_max_retry",
        dest="fault_tolerance_max_retry",
        type=int,
        default=3,
        help="自动容错最大重试次数（默认 3，仅在启用自动容错时生效）",
    )
    create_parser.add_argument("--no-track", action="store_true", help="不自动追踪任务")
    create_parser.add_argument("--dry-run", action="store_true", help="只显示 payload 不提交")
    create_parser.add_argument("--json", dest="output_json", action="store_true", help="输出 JSON 格式（供脚本集成）")

    # create-hpc 命令 - 创建 HPC 任务
    create_hpc_parser = subparsers.add_parser("create-hpc", aliases=["create-hpc-job"], help="创建并提交 HPC 任务到启智平台")
    create_hpc_parser.add_argument("--name", "-n", required=True, help="任务名称")
    create_hpc_parser.add_argument("--entrypoint", "--command", "-c", dest="entrypoint", required=True, help="HPC 入口命令")
    create_hpc_parser.add_argument("--workspace", "-w", help="工作空间 ID 或名称（从 qzcli catalog 缓存解析）")
    create_hpc_parser.add_argument("--project", "-p", help="项目 ID 或名称（仅唯一候选时自动选择）")
    create_hpc_parser.add_argument("--compute-group", "-g", dest="compute_group", help="计算组 ID 或名称（仅唯一候选时自动选择）")
    create_hpc_parser.add_argument("--spec", "-s", help="资源规格 ID（仅唯一候选时自动选择）")
    create_hpc_parser.add_argument("--image", "-i", required=True, help="Docker 镜像")
    create_hpc_parser.add_argument("--image-type", dest="image_type", default="SOURCE_PRIVATE", help="镜像类型（默认 SOURCE_PRIVATE）")
    create_hpc_parser.add_argument("--instances", "--instance-count", dest="instances", type=int, default=1, help="实例数量（默认 1）")
    create_hpc_parser.add_argument("--number-of-tasks", type=int, default=1, help="任务总数（默认 1）")
    create_hpc_parser.add_argument("--cpus-per-task", type=int, default=1, help="每个任务的 CPU 数（默认 1）")
    create_hpc_parser.add_argument("--memory-per-cpu", required=True, help="每个 CPU 的内存，例如 8Gi")
    create_hpc_parser.add_argument("--enable-hyper-threading", action="store_true", dest="enable_hyper_threading", help="启用超线程")
    create_hpc_parser.add_argument("--disable-hyper-threading", action="store_false", dest="enable_hyper_threading", help="禁用超线程")
    create_hpc_parser.set_defaults(enable_hyper_threading=False)
    create_hpc_parser.add_argument("--track", action="store_true", help="写入本地追踪（当前状态刷新未单独适配 HPC 任务）")
    create_hpc_parser.add_argument("--dry-run", action="store_true", help="只显示 payload 不提交")
    create_hpc_parser.add_argument("--json", dest="output_json", action="store_true", help="输出 JSON 格式（供脚本集成）")

    # batch 命令 - 批量提交任务
    batch_parser = subparsers.add_parser("batch", help="从 JSON 配置文件批量提交任务")
    batch_parser.add_argument("config", help="批量配置文件路径（JSON 格式）")
    batch_parser.add_argument("--dry-run", action="store_true", help="只预览不提交")
    batch_parser.add_argument("--delay", type=float, default=3, help="任务间延迟秒数（默认 3）")
    batch_parser.add_argument("--continue-on-error", action="store_true", help="遇到错误继续提交")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 0
    
    # 命令分发
    commands = {
        "init": cmd_init,
        "list": cmd_list,
        "ls": cmd_list,
        "status": cmd_status,
        "st": cmd_status,
        "stop": cmd_stop,
        "watch": cmd_watch,
        "w": cmd_watch,
        "track": cmd_track,
        "import": cmd_import,
        "remove": cmd_remove,
        "rm": cmd_remove,
        "clear": cmd_clear,
        "cookie": cmd_cookie,
        "login": cmd_login,
        "workspace": cmd_workspace,
        "ws": cmd_workspace,
        "catalog": cmd_workspaces,
        "workspaces": cmd_workspaces,
        "lsws": cmd_workspaces,
        "resources": cmd_workspaces,
        "res": cmd_workspaces,
        "avail": cmd_avail,
        "av": cmd_avail,
        "usage": cmd_usage,
        "tasks": cmd_task_dimensions,
        "jobs": cmd_task_dimensions,
        "blame": cmd_task_dimensions,
        "create": cmd_create,
        "create-job": cmd_create,
        "create-hpc": cmd_create_hpc,
        "create-hpc-job": cmd_create_hpc,
        "batch": cmd_batch,
    }
    
    cmd_func = commands.get(args.command)
    if cmd_func:
        try:
            return cmd_func(args)
        except KeyboardInterrupt:
            print("\n操作已取消")
            return 130
        except Exception as e:
            display = get_display()
            display.print_error(str(e))
            return 1
    else:
        parser.print_help()
        return 1


if __name__ == "__main__":
    sys.exit(main())
