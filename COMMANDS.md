# qzcli 命令参考

本文档记录 `qzcli` 的所有子命令、别名、位置参数和可选参数。参数以当前 `argparse` 定义为准；更新 CLI 参数时请同步更新本文。

## 全局规则

- 查看帮助：`qzcli --help` 或 `qzcli <command> --help`
- 查看版本：`qzcli --version` 或 `qzcli -V`
- 工作空间参数通常支持工作空间 ID 或名称；名称解析依赖 `qzcli catalog -u` 生成的本地缓存。
- 查看排队任务只能使用 `qzcli user-jobs --created-by USER_ID --queued`。这个统一入口会同时查询 `/api/v1/train_job/list` 和 `/api/v1/hpc_jobs/list`。
- `tasks` / `jobs` / `blame` 使用 `/api/v1/cluster_metric/list_task_dimension`，只代表工作空间当前资源占用视图，不能用来判断排队队列。
- `hpc-jobs` 是 HPC 单视图历史接口，可用于排查 HPC 运行/历史状态，但不作为“某人所有排队任务”的口径。

## 命令总览

| 命令 | 别名 | 用途 |
|---|---|---|
| `init` | 无 | 保存用户名、密码等基础配置 |
| `login` | 无 | 通过 CAS 登录并保存 cookie |
| `cookie` | 无 | 手动设置、查看或清除浏览器 cookie |
| `catalog` | `workspaces`, `lsws`, `res`, `resources` | 查看、刷新工作空间资源目录 |
| `avail` | `av` | 查询计算组空余节点和可用资源 |
| `specs` | `spec` | 查询分区可用资源规格 |
| `list` | `ls` | 查看本地或 cookie API 任务列表 |
| `hpc-jobs` | `hpc-list` | 查看 HPC 单视图任务历史 |
| `user-jobs` | `user-tasks` | 按 `created_by` 聚合查询用户任务，包含分布式训练和 HPC |
| `tasks` | `jobs`, `blame` | 查看 task dimension 当前资源占用视图 |
| `workspace` | `ws` | 查看单个工作空间内运行任务 |
| `usage` | 无 | 统计工作空间 GPU 使用分布 |
| `status` | `st` | 查看单个任务状态 |
| `stop` | 无 | 停止任务 |
| `watch` | `w` | 实时监控任务列表 |
| `track` | 无 | 把任务写入本地追踪 |
| `import` | 无 | 从文件导入任务 ID |
| `remove` | `rm` | 删除本地任务记录 |
| `clear` | 无 | 清空本地任务记录 |
| `create` | `create-job` | 创建分布式训练任务 |
| `create-hpc` | `create-hpc-job` | 创建 HPC 任务 |
| `batch` | 无 | 从 JSON 配置批量提交任务 |

## 认证与配置

### `qzcli init`

保存基础认证配置。后续 API token 登录会读取这些配置，也可用环境变量覆盖。

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--username`, `-u` | 否 | 用户名 |
| `--password`, `-p` | 否 | 密码 |

### `qzcli login`

通过 CAS 统一认证登录并保存 cookie，推荐日常使用。

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--username`, `-u` | 否 | 学工号 |
| `--password`, `-p` | 否 | 密码；含特殊字符时建议用单引号或 `--password-stdin` |
| `--password-stdin` | 否 | 从 stdin 读取密码，适合脚本 |
| `--workspace`, `-w` | 否 | 保存默认工作空间 ID |

### `qzcli cookie [cookie]`

手动管理浏览器 cookie。只有在无法自动登录或需要复制浏览器会话时使用。

| 参数 | 必填 | 用途 |
|---|---:|---|
| `cookie` | 否 | 浏览器 cookie 字符串 |
| `--file`, `-f` | 否 | 从文件读取 cookie |
| `--workspace`, `-w` | 否 | 保存默认工作空间 ID |
| `--show` | 否 | 显示当前 cookie 摘要 |
| `--clear` | 否 | 清除本地 cookie |
| `--no-test` | 否 | 保存时不测试 cookie 有效性 |

## 资源查询

### `qzcli catalog`

查看和刷新工作空间资源目录，包括项目、计算组、规格。本地名称解析依赖这个缓存。

别名：`workspaces`, `lsws`, `res`, `resources`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--workspace`, `-w` | 否 | 工作空间 ID 或名称 |
| `--export`, `-e` | 否 | 输出可用于脚本的环境变量格式 |
| `--update`, `-u` | 否 | 强制从 API 更新缓存 |
| `--list`, `-l` | 否 | 列出所有已缓存工作空间 |
| `--name` | 否 | 为工作空间设置本地别名 |

### `qzcli avail`

查询计算组空余节点，帮助决定任务提交到哪里。

别名：`av`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--workspace`, `-w` | 否 | 工作空间 ID 或名称 |
| `--group`, `-g` | 否 | 计算组 ID 或名称；不指定则查询所有 |
| `--nodes`, `-n` | 否 | 需要的节点数；用于筛选满足需求的计算组 |
| `--export`, `-e` | 否 | 输出可用于脚本的环境变量格式 |
| `--verbose`, `-v` | 否 | 显示空闲节点名称列表 |
| `--lp`, `--low-priority` | 否 | 计算低优任务占用节点，较慢 |
| `--cpu` | 否 | 按节点类型统计 CPU/MEM 空闲资源 |
| `--cpu-th` | 否 | CPU/MEM 阈值，格式 `cpu,mem`；可重复 |
| `--cpu-page-size` | 否 | CPU 统计模式节点分页大小，默认 200 |

### `qzcli specs`

查询分区下可用资源规格。

别名：`spec`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--workspace`, `-w` | 否 | 工作空间 ID 或名称；不指定则查所有已缓存工作空间 |
| `--group`, `-g` | 否 | 计算组/分区 ID 或名称；不指定则查工作空间内所有缓存分区 |
| `--all-workspaces`, `-a` | 否 | 查询所有已缓存工作空间 |
| `--schedule-config-type` | 否 | 调度配置类型，默认 `SCHEDULE_CONFIG_TYPE_HPC` |
| `--summary` | 否 | 只显示每个分区的规格数量 |
| `--json` | 否 | 输出 JSON |

## 任务查询

### `qzcli list`

列出任务。默认使用本地 store；加 `--cookie` 后从 API 拉取。

别名：`ls`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--limit`, `-n` | 否 | 显示数量限制，默认 20 |
| `--status`, `-s` | 否 | 按归一化状态过滤 |
| `--running`, `-r` | 否 | 只显示运行中/排队中的任务 |
| `--no-refresh` | 否 | 不更新状态 |
| `--verbose`, `-v` | 否 | 显示详细信息 |
| `--url`, `-u` | 否 | 显示任务链接，默认开启 |
| `--wide` | 否 | 宽格式显示，默认开启 |
| `--compact` | 否 | 紧凑表格格式，会关闭宽格式 |
| `--cookie`, `-c` | 否 | 使用 cookie 从 API 获取任务，不依赖本地 store |
| `--workspace`, `-w` | 否 | Cookie 模式下指定工作空间 ID 或名称 |
| `--all-ws` | 否 | Cookie 模式下查询所有已缓存工作空间 |

### `qzcli hpc-jobs`

查看 `/api/v1/hpc_jobs/list` 的 HPC 单视图任务历史。它可以按状态或创建者过滤，但查看某人的排队任务时仍必须用 `user-jobs --queued`。

别名：`hpc-list`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--workspace`, `-w` | 否 | 工作空间 ID 或名称 |
| `--created-by` | 否 | 创建者用户 ID |
| `--status`, `-s` | 否 | 按原始状态过滤，如 `QUEUEING`、`RUNNING`、`SUCCEEDED` |
| `--queued`, `-q` | 否 | 只显示 HPC 单视图里的 `QUEUEING`，仅用于诊断 |
| `--running`, `-r` | 否 | 显示 `QUEUEING`、`RUNNING`、`PENDING`、`CREATING` |
| `--page` | 否 | 页码，默认 1 |
| `--limit`, `-n` | 否 | 显示数量限制，默认 20 |
| `--verbose`, `-v` | 否 | 显示详细信息 |
| `--url`, `-u` | 否 | 显示任务链接，默认开启 |
| `--wide` | 否 | 宽格式显示，默认开启 |
| `--compact` | 否 | 紧凑表格格式，会关闭宽格式 |
| `--json` | 否 | 输出 JSON |

### `qzcli user-jobs`

按 `created_by` 查询一个用户的完整任务视图。默认跨所有可访问工作空间，同时查分布式训练和 HPC。查看排队任务只能使用这个命令。

别名：`user-tasks`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--created-by` | 是 | 创建者用户 ID |
| `--workspace`, `-w` | 否 | 工作空间 ID 或名称；不指定则查询所有可访问工作空间 |
| `--kind` | 否 | 查询视图，`all`、`train` 或 `hpc`，默认 `all` |
| `--status`, `-s` | 否 | 按原始状态过滤，如 `QUEUEING`、`RUNNING`、`CREATING` |
| `--queued`, `-q` | 否 | 只显示排队/创建中的任务 |
| `--running`, `-r` | 否 | 显示运行中/排队/创建中的任务 |
| `--limit`, `-n` | 否 | 显示数量限制，默认 50 |
| `--page-size` | 否 | 每页数量，默认 100 |
| `--max-pages` | 否 | 每个工作空间每个接口最多翻页数，默认 5；`0` 表示不限制 |
| `--json` | 否 | 输出 JSON |

常用示例：

```bash
qzcli user-jobs --created-by user-xxx --queued
qzcli user-jobs --created-by user-xxx --running --json
qzcli user-jobs --created-by user-xxx -w CPU资源空间 --kind hpc
```

### `qzcli tasks`

查看 `/api/v1/cluster_metric/list_task_dimension` 当前资源占用视图。适合看工作空间里当前运行/占资源任务、用户资源占用、项目占用和节点占用。不用于排队队列判断。

别名：`jobs`, `blame`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--workspace`, `-w` | 否 | 工作空间 ID 或名称 |
| `--project`, `-p` | 否 | 项目 ID 或名称 |
| `--page-size` | 否 | 后端分页大小，默认 100 |
| `--serve` | 否 | 启动本地前端，默认开启 |
| `--no-serve` | 否 | 只输出命令行表格，不启动前端 |
| `--host` | 否 | 前端监听地址，默认 `127.0.0.1` |
| `--port` | 否 | 前端监听端口，默认 `8765` |

### `qzcli workspace`

查看工作空间内运行任务，偏单工作空间 CLI 输出。

别名：`ws`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--workspace`, `-w` | 否 | 工作空间 ID 或名称 |
| `--project`, `-p` | 否 | 按项目名称过滤 |
| `--all`, `-a` | 否 | 显示所有项目，兼容旧参数，默认已不过滤 |
| `--page` | 否 | 页码，默认 1 |
| `--size` | 否 | 每页数量，默认 100 |
| `--sync`, `-s` | 否 | 同步到本地任务列表 |

### `qzcli usage`

统计工作空间 GPU 使用分布。

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--workspace`, `-w` | 否 | 工作空间 ID 或名称 |
| `--by-user`, `-u` | 否 | 按用户统计 GPU 使用 |
| `--by-project`, `-p` | 否 | 按项目统计 GPU 使用 |
| `--by-type`, `-t` | 否 | 按任务类型统计 |
| `--by-priority`, `-r` | 否 | 按优先级统计 |

## 任务控制与本地记录

### `qzcli status JOB_ID`

查看单个任务状态。

别名：`st`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `JOB_ID` | 是 | 任务 ID |
| `--json`, `-j` | 否 | 输出 JSON |

### `qzcli stop JOB_ID`

停止任务。

| 参数 | 必填 | 用途 |
|---|---:|---|
| `JOB_ID` | 是 | 任务 ID |
| `--yes`, `-y` | 否 | 跳过确认 |

### `qzcli watch`

实时监控任务列表。

别名：`w`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--interval`, `-i` | 否 | 刷新间隔秒数，默认 10 |
| `--limit`, `-n` | 否 | 显示数量限制，默认 30 |
| `--keep-alive`, `-k` | 否 | 所有任务完成后继续监控 |

### `qzcli track JOB_ID`

把任务写入本地追踪列表，常用于脚本提交后登记任务。

| 参数 | 必填 | 用途 |
|---|---:|---|
| `JOB_ID` | 是 | 任务 ID |
| `--name` | 否 | 任务名称 |
| `--source` | 否 | 来源脚本 |
| `--workspace` | 否 | 工作空间 ID |
| `--quiet`, `-q` | 否 | 静默模式 |

### `qzcli import FILE`

从文件导入任务 ID。

| 参数 | 必填 | 用途 |
|---|---:|---|
| `FILE` | 是 | 包含任务 ID 的文件 |
| `--source` | 否 | 来源标记 |
| `--refresh`, `-r` | 否 | 导入后更新状态 |

### `qzcli remove JOB_ID`

删除本地任务记录，不等同于停止远端任务。

别名：`rm`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `JOB_ID` | 是 | 任务 ID |
| `--yes`, `-y` | 否 | 跳过确认 |

### `qzcli clear`

清空所有本地任务记录。

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--yes`, `-y` | 否 | 跳过确认 |

## 创建任务

### `qzcli create`

创建并提交分布式训练任务。

别名：`create-job`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--name`, `-n` | 是 | 任务名称 |
| `--command`, `-c` | 是 | 执行命令 |
| `--workspace`, `-w` | 否 | 工作空间 ID 或名称 |
| `--project`, `-p` | 否 | 项目 ID 或名称；仅唯一候选时自动选择 |
| `--compute-group`, `-g` | 否 | 计算组 ID 或名称；仅唯一候选时自动选择 |
| `--spec`, `-s` | 否 | 资源规格 ID；仅唯一候选时自动选择 |
| `--image`, `-i` | 否 | Docker 镜像 |
| `--image-type` | 否 | 镜像类型，默认 `SOURCE_PRIVATE` |
| `--instances` | 否 | 实例数量，默认 1 |
| `--shm` | 否 | 共享内存 GiB，默认 1200 |
| `--priority` | 否 | 任务优先级 1-10，默认 10 |
| `--framework` | 否 | 框架类型，默认 `pytorch` |
| `--auto-fault-tolerance`, `--auto_fault_tolerance` | 否 | 启用自动容错 |
| `--fault-tolerance-max-retry`, `--fault_tolerance_max_retry` | 否 | 自动容错最大重试次数，默认 3 |
| `--no-track` | 否 | 不自动追踪任务 |
| `--dry-run` | 否 | 只显示 payload，不提交 |
| `--json` | 否 | 输出 JSON |

### `qzcli create-hpc`

创建并提交 HPC 任务。

别名：`create-hpc-job`

| 参数 | 必填 | 用途 |
|---|---:|---|
| `--name`, `-n` | 是 | 任务名称 |
| `--entrypoint`, `--command`, `-c` | 是 | HPC 入口命令 |
| `--workspace`, `-w` | 否 | 工作空间 ID 或名称 |
| `--project`, `-p` | 否 | 项目 ID 或名称；仅唯一候选时自动选择 |
| `--compute-group`, `-g` | 否 | 计算组 ID 或名称；仅唯一候选时自动选择 |
| `--spec`, `-s` | 否 | 资源规格 ID；仅唯一候选时自动选择 |
| `--image`, `-i` | 是 | Docker 镜像 |
| `--image-type` | 否 | 镜像类型，默认 `SOURCE_PRIVATE` |
| `--instances`, `--instance-count` | 否 | 实例数量，默认 1 |
| `--number-of-tasks` | 否 | 任务总数，默认 1 |
| `--cpus-per-task` | 否 | 每个任务的 CPU 数，默认 1 |
| `--memory-per-cpu` | 是 | 每个 CPU 的内存，例如 `8Gi` |
| `--ttl-after-finish-seconds` | 否 | 任务完成后保留秒数，默认 600 |
| `--enable-hyper-threading` | 否 | 启用超线程 |
| `--disable-hyper-threading` | 否 | 禁用超线程，默认禁用 |
| `--track` | 否 | 写入本地追踪 |
| `--dry-run` | 否 | 只显示 payload，不提交 |
| `--json` | 否 | 输出 JSON |

### `qzcli batch CONFIG`

从 JSON 配置文件批量提交任务。

| 参数 | 必填 | 用途 |
|---|---:|---|
| `CONFIG` | 是 | 批量配置文件路径 |
| `--dry-run` | 否 | 只预览，不提交 |
| `--delay` | 否 | 任务间延迟秒数，默认 3 |
| `--continue-on-error` | 否 | 遇到错误继续提交 |
