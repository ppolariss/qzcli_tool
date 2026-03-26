"""
创建任务相关命令。
"""

import argparse
import time
from pathlib import Path

from .api import QzAPIError, get_api
from .display import get_display
from .resource_resolution import ResourceResolutionError, resolve_create_refs
from .store import JobRecord, get_store


def resolve_create_context(args, display):
    """Resolve workspace, project, compute group and spec for create-style commands."""
    if not args.workspace:
        display.print_error("请指定工作空间: --workspace <名称或ID>")
        display.print("[dim]使用 qzcli catalog --list 查看已缓存的工作空间[/dim]")
        return None
    try:
        ctx = resolve_create_refs(
            workspace=args.workspace,
            project=args.project or "",
            compute_group=args.compute_group or "",
            spec=args.spec or "",
        )
    except ResourceResolutionError as e:
        display.print_error(str(e))
        if args.workspace:
            display.print("[dim]使用 qzcli catalog -w <workspace> 查看可用资源[/dim]")
        return None

    if ctx.auto_project:
        display.print(f"[dim]自动选择项目: {ctx.project_display} ({ctx.project_id})[/dim]")
    if ctx.auto_compute_group:
        display.print(f"[dim]自动选择计算组: {ctx.compute_group_display} ({ctx.compute_group_id})[/dim]")
    if ctx.auto_spec:
        display.print(f"[dim]自动选择规格: {ctx.spec_display} ({ctx.spec_id})[/dim]")

    return {
        "workspace_id": ctx.workspace_id,
        "workspace_display": ctx.workspace_display,
        "project_id": ctx.project_id,
        "project_display": ctx.project_display,
        "compute_group_id": ctx.compute_group_id,
        "compute_group_display": ctx.compute_group_display,
        "spec_id": ctx.spec_id,
        "spec_display": ctx.spec_display,
    }


def cmd_create(args):
    """创建任务"""
    display = get_display()
    api = get_api()
    store = get_store()
    ctx = resolve_create_context(args, display)
    if not ctx:
        return 1

    payload = {
        "name": args.name,
        "logic_compute_group_id": ctx["compute_group_id"],
        "project_id": ctx["project_id"],
        "workspace_id": ctx["workspace_id"],
        "framework": args.framework,
        "command": args.cmd_str,
        "task_priority": args.priority,
        "auto_fault_tolerance": False,
        "framework_config": [{
            "spec_id": ctx["spec_id"],
            "image": args.image,
            "image_type": args.image_type,
            "instance_count": args.instances,
            "shm_gi": args.shm,
        }],
    }

    if args.dry_run:
        import json as json_mod
        display.print("[bold]Dry run - 以下为将要提交的 payload:[/bold]\n")
        print(json_mod.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    display.print(f"\n[bold]创建任务[/bold]")
    display.print(f"  名称: {args.name}")
    display.print(f"  工作空间: {ctx['workspace_display']} ({ctx['workspace_id']})")
    display.print(f"  项目: {ctx['project_display']} ({ctx['project_id']})")
    display.print(f"  计算组: {ctx['compute_group_display']} ({ctx['compute_group_id']})")
    display.print(f"  规格: {ctx['spec_display']} ({ctx['spec_id']})")
    display.print(f"  镜像: {args.image}")
    display.print(f"  实例数: {args.instances}")
    display.print(f"  共享内存: {args.shm} GiB")
    display.print(f"  优先级: {args.priority}")
    display.print(f"  命令: {args.cmd_str[:120]}{'...' if len(args.cmd_str) > 120 else ''}")
    display.print("")

    try:
        result = api.create_job(payload)
    except QzAPIError as e:
        display.print_error(f"任务创建失败: {e}")
        return 1

    job_id = result.get("job_id", "")
    resp_ws_id = result.get("workspace_id", ctx["workspace_id"])

    if not job_id:
        display.print_error("任务创建失败: 响应中未包含 job_id")
        if args.output_json:
            import json as json_mod
            print(json_mod.dumps(result, indent=2, ensure_ascii=False))
        return 1

    job_url = f"https://qz.sii.edu.cn/jobs/distributedTrainingDetail/{job_id}?spaceId={resp_ws_id}"

    display.print_success("任务创建成功!")
    display.print(f"  Job ID: [cyan]{job_id}[/cyan]")
    display.print(f"  链接: {job_url}")

    if not args.no_track:
        job = JobRecord(
            job_id=job_id,
            name=args.name,
            status="job_pending",
            workspace_id=resp_ws_id,
            project_id=ctx["project_id"],
            source="qzcli create",
            command=args.cmd_str,
            url=job_url,
            instance_count=args.instances,
            priority_level=str(args.priority),
        )
        store.add(job)
        display.print("  [dim]已自动追踪到本地[/dim]")

    if args.output_json:
        import json as json_mod
        output = {
            "job_id": job_id,
            "workspace_id": resp_ws_id,
            "url": job_url,
            "name": args.name,
        }
        print(json_mod.dumps(output, ensure_ascii=False))

    return 0


def cmd_create_hpc(args):
    """创建 HPC 任务"""
    display = get_display()
    api = get_api()
    store = get_store()

    ctx = resolve_create_context(args, display)
    if not ctx:
        return 1

    payload = {
        "name": args.name,
        "logic_compute_group_id": ctx["compute_group_id"],
        "project_id": ctx["project_id"],
        "workspace_id": ctx["workspace_id"],
        "entrypoint": args.entrypoint,
        "image": args.image,
        "image_type": args.image_type,
        "instance_count": args.instances,
        "spec_id": ctx["spec_id"],
        "number_of_tasks": args.number_of_tasks,
        "cpus_per_task": args.cpus_per_task,
        "memory_per_cpu": args.memory_per_cpu,
        "enable_hyper_threading": args.enable_hyper_threading,
    }

    if args.dry_run:
        import json as json_mod
        display.print("[bold]Dry run - 以下为将要提交的 HPC payload:[/bold]\n")
        print(json_mod.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    display.print(f"\n[bold]创建 HPC 任务[/bold]")
    display.print(f"  名称: {args.name}")
    display.print(f"  工作空间: {ctx['workspace_display']} ({ctx['workspace_id']})")
    display.print(f"  项目: {ctx['project_display']} ({ctx['project_id']})")
    display.print(f"  计算组: {ctx['compute_group_display']} ({ctx['compute_group_id']})")
    display.print(f"  规格: {ctx['spec_display']} ({ctx['spec_id']})")
    display.print(f"  镜像: {args.image}")
    display.print(f"  实例数: {args.instances}")
    display.print(f"  任务数: {args.number_of_tasks}")
    display.print(f"  每任务 CPU: {args.cpus_per_task}")
    display.print(f"  每 CPU 内存: {args.memory_per_cpu}")
    display.print(f"  超线程: {'开启' if args.enable_hyper_threading else '关闭'}")
    display.print(f"  入口命令: {args.entrypoint[:120]}{'...' if len(args.entrypoint) > 120 else ''}")
    display.print("")

    try:
        result = api.create_hpc_job(payload)
    except QzAPIError as e:
        display.print_error(f"HPC 任务创建失败: {e}")
        return 1

    job_id = result.get("job_id", "")
    resp_ws_id = result.get("workspace_id", ctx["workspace_id"])

    if not job_id:
        display.print_error("HPC 任务创建失败: 响应中未包含 job_id")
        if args.output_json:
            import json as json_mod
            print(json_mod.dumps(result, indent=2, ensure_ascii=False))
        return 1

    job_url = f"{api.base_url}/jobs/distributedTrainingDetail/{job_id}?spaceId={resp_ws_id}"

    display.print_success("HPC 任务创建成功!")
    display.print(f"  Job ID: [cyan]{job_id}[/cyan]")
    display.print(f"  链接: {job_url}")

    if args.track:
        job = JobRecord(
            job_id=job_id,
            name=args.name,
            status="job_pending",
            workspace_id=resp_ws_id,
            project_id=ctx["project_id"],
            source="qzcli create-hpc",
            command=args.entrypoint,
            url=job_url,
            instance_count=args.instances,
            metadata={
                "job_type": "hpc",
                "number_of_tasks": args.number_of_tasks,
                "cpus_per_task": args.cpus_per_task,
                "memory_per_cpu": args.memory_per_cpu,
                "enable_hyper_threading": args.enable_hyper_threading,
            },
        )
        store.add(job)
        display.print("  [dim]已写入本地追踪；当前状态刷新接口未单独适配 HPC 任务[/dim]")
    else:
        display.print("  [dim]默认未加入本地追踪；如需记录到本地请加 --track[/dim]")

    if args.output_json:
        import json as json_mod
        output = {
            "job_id": job_id,
            "workspace_id": resp_ws_id,
            "url": job_url,
            "name": args.name,
            "job_type": "hpc",
            "tracked": args.track,
        }
        print(json_mod.dumps(output, ensure_ascii=False))

    return 0


def cmd_batch(args):
    """批量提交任务"""
    import itertools
    import json as json_mod

    display = get_display()

    config_path = Path(args.config)
    if not config_path.exists():
        display.print_error(f"配置文件不存在: {config_path}")
        return 1

    with open(config_path, "r", encoding="utf-8") as f:
        config = json_mod.load(f)

    defaults = config.get("defaults", {})
    matrix = config.get("matrix", {})
    name_template = config.get("name_template", "job-{_index}")
    command_template = config.get("command_template", "")

    if not command_template:
        display.print_error("配置文件中缺少 command_template")
        return 1

    keys = list(matrix.keys())
    if not keys:
        display.print_error("配置文件中 matrix 为空")
        return 1

    values = [matrix[k] if isinstance(matrix[k], list) else [matrix[k]] for k in keys]
    combinations = list(itertools.product(*values))
    total = len(combinations)

    display.print(f"\n[bold]批量任务提交[/bold]")
    display.print(f"  配置文件: {config_path}")
    display.print(f"  矩阵维度: {' x '.join(f'{k}({len(matrix[k]) if isinstance(matrix[k], list) else 1})' for k in keys)}")
    display.print(f"  总任务数: {total}")
    display.print("")

    if args.dry_run:
        display.print("[bold]Dry run - 预览所有任务:[/bold]\n")

    successful = 0
    failed = 0
    failed_tasks = []

    for idx, combo in enumerate(combinations, 1):
        variables = dict(zip(keys, combo))
        variables["_index"] = idx
        for k, v in variables.items():
            if isinstance(v, str) and "/" in v:
                import os as os_mod
                variables[f"{k}_basename"] = os_mod.path.basename(v)

        try:
            job_name = name_template.format(**variables)
        except KeyError as e:
            display.print_warning(f"任务 {idx}: name_template 变量缺失: {e}")
            job_name = f"batch-job-{idx}"

        try:
            command = command_template.format(**variables)
        except KeyError as e:
            display.print_error(f"任务 {idx}: command_template 变量缺失: {e}")
            failed += 1
            failed_tasks.append(f"{idx}: template error {e}")
            continue

        if args.dry_run:
            display.print(f"  [{idx}/{total}] {job_name}")
            display.print(f"    命令: {command[:120]}{'...' if len(command) > 120 else ''}")
            display.print("")
            continue

        display.print(f"[bold][{idx}/{total}][/bold] 提交: {job_name}")

        create_args = argparse.Namespace(
            name=job_name,
            cmd_str=command,
            workspace=defaults.get("workspace", ""),
            project=defaults.get("project", ""),
            compute_group=defaults.get("compute_group", ""),
            spec=defaults.get("spec", ""),
            image=defaults.get("image", ""),
            image_type=defaults.get("image_type", "SOURCE_PRIVATE"),
            instances=defaults.get("instances", 1),
            shm=defaults.get("shm", 1200),
            priority=defaults.get("priority", 10),
            framework=defaults.get("framework", "pytorch"),
            no_track=False,
            dry_run=False,
            output_json=False,
        )

        ret = cmd_create(create_args)
        if ret == 0:
            successful += 1
        else:
            failed += 1
            failed_tasks.append(f"{idx}: {job_name}")
            if not args.continue_on_error:
                display.print_error("任务提交失败，停止批量提交（使用 --continue-on-error 忽略错误）")
                break

        if idx < total and not args.dry_run:
            time.sleep(args.delay)

    if args.dry_run:
        display.print(f"[bold]预览完成，共 {total} 个任务[/bold]")
        return 0

    display.print(f"\n[bold]批量提交完成[/bold]")
    display.print(f"  总任务数: {total}")
    display.print(f"  成功: {successful}")
    display.print(f"  失败: {failed}")

    if failed_tasks:
        display.print("\n[bold]失败的任务:[/bold]")
        for task in failed_tasks:
            display.print(f"  - {task}")
        return 1

    return 0
