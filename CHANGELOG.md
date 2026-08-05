# Changelog

## Unreleased

## v0.4.6 - 2026-08-05

### 默认优先级 10 → 3（行为变更，可恢复）

**先说方向**：数字越小优先级越低。实测 **1494 个真实任务**，跨全部工作空间一致：

| 提交值 | 存储值 | 档位 | 现网任务数 |
|---|---|---|---|
| 1 | 11 | LOW | 697 |
| 3 | 13 | LOW | — |
| 4 | 20 | NORMAL | 481 |
| 5 | 30 | HIGH | 2 |
| 9 | 34 | HIGH | 44 |
| 10 | 35 | HIGH | 269 |

**和 HPC 完全同向。** 此前 `api.py` 的注释写着「训练任务的 task_priority 是反的 ——
那边 10 表示低优」，是错的，而且这条错还进了 v0.4.4 的发布说明。照着看的人会把
最高优当低优提上去，直接和生产任务抢卡。已更正。

**默认值改 3 的理由**：不显式指定优先级的，多半是调试 / 试跑 / 脚本随手提的任务。
让这类任务默认拿最高优去抢生产的卡，是不合理的默认。

**向后兼容**：这对「原来不写 `--priority`、靠默认拿高优」的脚本是行为变更 ——
那些任务会从直接跑变成排队。现网 **21% 的任务跑在 HIGH 档**，面不小。所以给了一条
不用改调用点就能恢复原状的路，沿用已有的三级阶梯：

```
QZCLI_DEFAULT_PRIORITY → ~/.qzcli/.env → config.json 的 default_priority → 兜底 3
```

加一行 `QZCLI_DEFAULT_PRIORITY=10` 就完全恢复旧行为。用默认值时会打一行提示说清
用了哪档、怎么改 —— 行为变过就不能悄悄变。

### cookie 落盘不是原子的

`save_cookie` 原本是 `open(path,"w")` 然后 `json.dump`。截断和写完之间有个窗口
文件是空的；并发读的线程这时 `json.load` 失败，而所有读取点都把失败当成"没有这个
文件"，于是 `get_cookie()` 返回 `None`。

**实测后果**：8 个并发登录里偶发 **2 次真实 CAS 登录** —— 某个线程在窗口里读到
`None`，`_relogin` 的去重判据失效，又打了一次 CAS。而反复登录正是把账号推进验证码
锁定的动作。别处则可能表现为莫名的「未设置 cookie」。

改成临时文件 + `os.replace`（同文件系统原子）。token 缓存同样处理。

**发现过程值得记**：是并发用例随机红了一次，没当抖动放过去，插桩查到两次登录来自
同一处，才挖到根因。认证相关的用例一旦 flaky 就会被学会忽略，而这次它指向真问题。

### 规格解析：卡型跨计算组抄错

两层都有问题：`_fill_gpu_type_from_history` 按 quota_id 匹配历史但不看计算组；
`_lookup_spec_for_payload` 读缓存也不看归属。结果是给 H200 组提交却填
`NVIDIA_H100_SXM_80G`。

**这比报错更糟**：任务会一直排队等一种该组里根本不存在的卡，看起来"成功进入排队"，
实际永远起不来 —— 正好骗过"能排队就算通过"这类验收。

修法：历史按计算组过滤；查不到则用**该计算组节点的真实卡型**兜底（卡型是机器属性，
节点才是权威来源，对没跑过任务的新组也有效）；缓存记录不属于目标组时重新解析。

实测「训练区-H200-1号机房」+「8卡160核」：`H100` → `''` → **`NVIDIA_H200_SXM_141G`**，
与该组 180 个节点一致。

### 其它

- `README.md` 快速开始里的 `qzcli ls -c -r` 跑不通（`-c` 模式必须带 `-w` 或
  `--all-ws`）。这条错误还被抄进了给新同学的手册 —— 写文档抄 README 而不实跑，
  就是这个下场。
- `live_smoke` 新增「低优大任务能进排队」用例，带账号门控
  （`QZCLI_SMOKE_QUEUE_ACCOUNT`），并硬断言规格卡型与该计算组节点实际卡型一致 ——
  防的正是上面那种"假通过"。

### 测试 379 → 404

新增：优先级方向与默认值（含可覆盖的兼容通道）、cookie 原子写（并发读永远读不到
空值、并发写不互删临时文件）、规格归属与卡型来源。多条经过变异验证。

## v0.4.5 - 2026-08-04

主题是**登录治理**。用户报「登录多了就会挂」，系统审计下来真正的元凶不是
「session TTL 太短」，而是两处让登录白白发生的 bug。

从用户 shell 历史里挖到的数字最能说明问题：**`qzcli login` 被敲了 299 次**，
是所有 qzcli 命令里最多的。

### 分页循环里每页都在完整重登

`_relogin` 的去重判据是「盘上 cookie **相对我进函数那一刻**有没有变过」。而分页
函数把 cookie 闭包了，整个循环共用一个字符串：

1. 第 1 页 401 → 重登成功 → 盘上换成新 cookie
2. 第 2 页**仍用闭包里的旧 cookie** → 又 401
3. 此时 `stale` 读到的已经是新 cookie，`current == stale` → 去重判定为
   "没人刷新过" → **再打一次完整 CAS**

**N 页 = N 次登录。** 而 CAS 正是按登录次数判定异常并锁验证码 —— 这就是
"用着用着就说要验证码"的直接来源。`avail` / `usage` / `res -u` 全中。

两处一起改才堵死：判据改成「盘上 cookie ≠ **刚刚失败的那个**」；分页不再闭包
cookie，每页回源读盘。只改判据能把 CAS 压到 1 次但每页仍白撞一次 401；
只改分页挡不住线程错峰。

思路来自 **inspire-skill**：它的凭据是**可变对象**（`WebSession`），重登后
`_refresh_session_in_place` 原地改写调用方手里那个对象，持有引用的循环下次自然
用上新值。qzcli 全链路传裸字符串，对象化改造面太大，因此用「每次回源读盘」拿
等价效果。（顺带澄清：inspire-skill **没有**主动续期，`SESSION_TTL` 是死代码，
被 `load(allow_expired=True)` 绕过；它也**没有任何并发保护**。）

真机验证：7 页节点分页（698 节点）→ **登录 1 次**（修复前 7 次）。

### `qzcli create` 每次必然白登一次

`list_projects_raw()` 调用时不传 cookie，而 v1 这条路**没有磁盘兜底**（v2 的
`_request_v2` 有），空串直接进 header → 必然 401 → 触发一次纯属浪费的完整 CAS
登录。用户感觉"create 偶尔很慢"就是这个。

真机验证：不传 cookie 调用 → **登录 0 次**（修复前 1 次）。

### 429 被吞掉后转打 v1（同一个坑的第四处）

`get_job_detail` 用裸 `except QzAPIError: pass`，而 `QzRateLimitError` 是它的
子类 —— 429 被静默吞掉、转头再打一发 v1，**等于平台喊"慢点"时把请求量翻倍**。
`_v2_then_v1` 里明令禁止过这件事，但这是另一条独立路径。

### 其它同类修复

- **MCP `qz_auth_login` 零保护直连 `login_with_cas`**。`cmd_login` 修过、
  `_refresh_cookie_for_interactive` 修过，这是第三处。改走 `_relogin`。
- `_project_list_items` 上同一组装饰器**挂了两遍** → 429 重试 4×4=16 次。
- `cmd_login` 成功后不清失败冷却，手工登录成功后 60s 内自动重登仍被挡。
- 缓存里工作空间的值不是 dict 时（半截写入 / 手工编辑），
  `get_workspace_resources` 原样返回字符串，下游 `.get()` 抛 `AttributeError`
  **把整条命令打崩**。
- 两个 MCP tool 没写 description（`qz_create_hpc_job` / `qz_get_hpc_usage`），
  模型侧无从判断何时调用。

### 测试：318 → 379

- **缓存残缺矩阵**。契约是三态：`True` / `False` / `None`（缓存无从判断）。
  线上那批 bug 全是同一个形状 —— **该返回 `None` 时返回了 `False`**。
- **10 个零覆盖命令**（`remove` / `clear` / `track` / `import` / `cookie` /
  `watch`）。重点钉否定路径：回答 `n` 必须真的什么都不删、cookie 验证失败
  **绝不能**落盘顶掉正在用的好 cookie。
- **`mcp_server.py` 17 个 tool**，此前零测试。它是与 CLI 并列的第二个用户面，
  但几乎完全是平行重实现 —— CLI 的测试完全不保护它。
- **代理双栈一致性**。一条勘察结论被实测证伪：「只设 `HTTP_PROXY` 时两栈行为
  相反」不成立，两栈对 https 流量都正确忽略它。照报告去"修"反而会制造真分叉。
- **`live_smoke` 补 3 条默认形态**：`hpc-usage` / `list -c --all-ws` /
  `res -u`（8 线程扇出，429 风险最高）。

### 新工具：按用户真实命令做差分回放

`tools/replay_history.py` —— 从 shell 历史解析出真实命令分布，在两个代码版本上
分别回放并比对。这个项目栽过的坑根子都是"测我构造的路径，不测用户实际怎么用"。

比对基准换过三次：直接 diff 输出、抹掉数字、比行模板集合 —— **同一个版本自比
都通不过**（集群 99.9% 利用率，表格行数和列宽都在变）。结论是渲染后的表格不适合
当回归信号，改成比行为属性：退出码、429/权限噪声/异常栈计数、**触发了几次重新
登录**、以及结构锚点（不含数字的行）。自比对 0 差异之后，基准才站得住。

首轮：dev vs master 回放 10 条真实命令，**失败 0、差异 0**。

## v0.4.4 - 2026-08-01

一次真实事故驱动的版本：用户报「`qzcli login` 用正确密码却说需要输入验证码」，
跑去浏览器一看根本没有验证码可过。查下来是两个独立问题叠在一起。

### 「需要输入验证码」是假错误

判据是这一行：

```python
if "验证码" in resp.text:
    raise QzAPIError("需要输入验证码，请在浏览器中登录后手动获取 cookie")
```

而 CAS 登录页**永远**含"验证码"三个字 —— 抓真实页面数过，**整整 5 处**，全部
来自旁边那个「短信验证码登录」标签页的固定文案（`<h3>验证码登录</h3>`、
`placeholder="验证码"`、`发送验证码`、`动态验证码`）。其中那个图形验证码
`<img>` 指向的还是 `mapp.suda.edu.cn`（苏州大学），是模板里没清干净的死代码。

于是**任何**退回登录页的失败都被翻译成"需要输入验证码"，真实原因完全没被说出来，
反而诱导用户反复重试 —— 而重试正是让情况变糟的动作。

改成只读 `<div class="form-error">` 里的文案。拿到真实文案后才知道确实存在验证码，
但那是**短时间内登录失败几次后 CAS 才临时打开的**，等几分钟自行恢复。提示语相应
改成「等几分钟重试」，并指出已保存的 cookie 若仍有效根本无需重新登录 ——
而不是像以前那样叫用户去浏览器手工取 cookie（那等于承认密码登录坏了）。

### 自动重登在并发扇出下把账号打进验证码

`_refresh_cookie_for_interactive` **直接调 `login_with_cas`**，绕过了 `_relogin`
的全部三层保护（进程内锁、跨进程文件锁、拿到锁后重读 cookie 的去重）。

而它被 `_with_live_cookie` 调用，后者出现在 **11 个命令**里，其中 `avail` /
`usage` / `list -c` 都是按计算组**并发扇出**的。cookie 一过期，一条
`qzcli avail` 会朝 CAS 打出十几次并发登录。**实测：16 线程 → CAS 被打 16 次。**

v0.4.1 加的锁只覆盖了 `_relogin` 和 `qzcli login`，恰好漏掉这条最常触发的路径。

**失败路径还要单独处理**：登录失败时没有新 cookie 落盘，"重读看别人是否已登好"
的判据就永远为假，于是每个等在锁上的线程都会各自再打一次 —— 8 线程 8 次**失败**
尝试。而 CAS 正是按失败次数延长锁定的，等于自动重登把自己越锁越死。加了 60s 失败
冷却（进程内 + 跨进程冷却文件），成功即清除。

### 缓存写坏会把命令打崩

缓存里工作空间的值不是 dict 时（半截写入 / 手工编辑 / 老格式残留），
`get_workspace_resources` 原样返回字符串，下游 `.get(...)` 抛 `AttributeError`。
所有调用方本来就按"`None` 表示没缓存"处理，当作没缓存即可。

### 测试

**318 tests**（v0.4.3 是 271）。新增三组：

- **沙箱 HOME + 13 种缓存残缺态 fixture**。此前构造残缺缓存只能拿真实
  `~/.qzcli` 做实验，跑挂一次就把登录态留在半坏状态。结构照真实
  `resources.json` 抄的 —— 第一版照文档推的全错（顶层是扁平映射不是
  `{"workspaces": [...]}`，三个集合都是按 id 索引的 dict 不是 list）。
- **缓存残缺矩阵**。契约是三态：`True` / `False` / `None`（缓存无从判断）。
  线上那批 bug 全是同一个形状 —— **该返回 `None` 时返回了 `False`**。
- **并发重登**。`avail` 本来有 5 个单测、`live_smoke` 也跑它，但**没有一个是在
  cookie 过期的前提下跑的**，而放大只发生在认证失败那条路上。

### 平台侧缺口（已记入功能点表）

实测 `inspire-session` 是会话级 cookie（无 Expires/Max-Age），**约 20 分钟即失效**
（两次分别测到 18.6 / 22 分钟）；且服务端**不下发续期 cookie** —— 连打 3 次 v2
接口外加 v1 对照，`Set-Cookie` 全部为空，即不是滑动过期。会话很快到期 → 客户端
必须频繁重登 → 重登又容易触发验证码。客户端这侧能做的已经做了，会话 TTL 属平台
侧策略。

### 流程

`master` 改为只收发布，日常改动走 `dev`（启智不少生产基建直接依赖 `master`）。
进 `master` 的门槛写成硬清单：单测 + `live_smoke` + `parity_sweep` 的 SCHEMA
差异必须为 0 + 发版前跑一次 `--submit`。

本次发版实跑记录：318 tests / parity **0 处差异（SCHEMA 0）** / live_smoke **17/17**。

`parity_sweep` 顺带把 `tasks_associated`、`users_associated` 归入波动字段 ——
定性方法是**拿 v1 跟 v1 自己比**：同源相隔 150 秒就有 2/100 个节点在变，
而同一时刻 v1 vs v2 是 100/100 一致，说明差异来自采样时刻不同。

## v0.4.3 - 2026-07-31

一次系统性审计挖出的 5 个问题，全部是「看起来在工作但实际没起作用」。
每个都先写会红的复现用例、再修到绿。

### create 相关（两个和之前修过的是同构代码）

- **项目归属校验只修了一半**：项目和计算组的归属校验本来是同一套逻辑，
  v0.4.2 只给计算组加了平台复核。于是**新建/新加入的项目会原样重演那个 bug** ——
  报「项目 X 不属于当前工作空间」，而它其实属于。补上同样的平台复核
  （数据源 `/api/v1/project/list` 的 `items[].space_list[]`）。
- **不带 `--spec` 在 15/16 个工作空间是坏的**：`res -u` 默认 quick 模式**明确
  不产出 specs**，所以 `specs={}` 是默认稳态。自动选规格只看缓存 → 绝大多数
  工作空间直接报「未指定资源规格且缓存中无可用规格」。接上 v0.4.0 已有的平台
  规格表，且挑 GPU 数最小的（别默认占最大机器）；缓存有则维持原行为。

### 另外三个

- **`batch --dry-run` 完全不校验资源**：展开完模板就跳过，压根不走 `cmd_create`，
  workspace / project / compute-group / spec 能不能解析一概不管。用户拿它当
  提交前预检必然翻车。改成走完整链路，并**把 `dry_run` 透传下去**
  （原来写死 `False`，不透传的话这个修复会让预检变成真提交）。
- **跨进程重登锁没覆盖 `qzcli login` 本身**：v0.4.1 的锁只保护自动重登，
  显式 login 没拿 —— 多个 agent 同时敲 login 仍会并发撞 CAS。
  实测修复后 **5 个进程并发只打到 CAS 1 次**。
- **`live_smoke` 解包了 rc 却从不断言**：命令 exit 1 但输出恰好没有关键词
  就算通过，用例可以静默失效。

### 新增：v1/v2 全量对齐扫描（`tools/parity_sweep.py`）

趁 v1 还没下线，把两边的语义差异一次性挖出来，而不是等用户撞 bug 反推。
全部已迁端点 × 全部工作空间 × 逐字段逐值，SCHEMA 类差异用退出码暴露，可当 gate。

**首次全量结果：96 对比对，0 处真差异。** 唯一发现的实质差异是
`gpu_info.brand_name`（v1 空串 / v2 "英伟达"），代码不读该字段，已核实无害。
**SCHEMA = 0**，即不存在「字段改名导致静默返回空」这类隐患 ——
v1 下线对这些端点是安全的。

271 tests passed。

## v0.4.2 - 2026-07-31

### 新建的计算组会被误判成「不属于当前工作空间」

`create` 校验计算组归属时**只看本地缓存**。而缓存总会过期 —— 新建的计算组在
刷新前必然查不到，于是一个**真实存在、此刻正跑着千卡任务**的计算组被判成
「不属于当前工作空间」。这句报错本身就是错的，而且它建议的 `res -u` 也未必
解决（缓存刷新有自己的失败模式）。

改为：缓存说「没有」时**跟平台再确认一次**
（`workspace ListLogicComputeGroups` 是权威来源、不依赖缓存）：

- 平台确认存在 → 放行，并提示缓存已过期
- 平台确认不存在 → 照常拒绝（报错措辞改成「已向平台确认」）
- **查询失败 → 按「不确定」放行**，让平台自己去拒 —— 总好过拿过期缓存误伤

注意这和 v0.4.0 修的规格解析是**两个不同的故障**：那个报
「无法解析规格 ... 的 cpu/gpu/memory」，这个报「计算组 ... 不属于当前工作空间」。
两处都得治。

258 tests passed。

## v0.4.1 - 2026-07-31

紧急修复三个线上问题，全部来自真实用户反馈。

### `qzcli avail` 默认形态全线 HTTP 429

不带 `-w` 时是「工作空间 × 计算组」的嵌套并发，撞上 APISIX 限流。

根因是 v0.4.0 的回落逻辑把问题放大了：APISIX 限流返回 **429 + HTML 错误页**，
`_request_v2` 先嗅 content-type 判成「返回非 JSON」→ `_v2_then_v1` 当成路由不通
→ **回落 v1** → v1 也 429 → 全灭。**平台在喊「慢点」，代码却把 QPS 翻倍。**

改为：新增 `QzRateLimitError`，在 content-type 嗅探**之前**判 429；退避重试
（优先听 `Retry-After`，否则 1s→2s→4s 叠抖动）；**429 绝不回落 v1**。
v1 的 12 个请求点同样识别 429。

### 多 agent 并发把账号撞进 CAS 验证码

`_relogin` 只有进程内的 `threading.Lock`，而每次 `qzcli` 调用都是独立进程。
cookie 一过期，N 个进程同时撞 CAS，被判为异常登录要求验证码，
**所有人一起被锁在外面**，连「自动重登」本身也失效。

新增 `~/.qzcli/.relogin.lock` 文件锁（`flock`，进程被 kill -9 时内核自动释放）。
拿到锁后重读盘上的 cookie —— 别的进程可能刚登好了，全程只发生一次 CAS 登录。
实测 8 个并发进程只触发 1 次登录。

### 已禁用/无权限的工作空间刷屏

`avail` / `usage` / `hpc-usage` 从**本地缓存**枚举工作空间，v0.4.0 的
`usage_status` 过滤只在 `list_workspaces` 生效、没管缓存这条路，于是每次都去查
这些空间并刷一屏 `AccessForbidden`，把真问题淹掉。改为撞到就打标、后续跳过，
`res -u` 重刷时清标记。实测警告 5 条 → 0。

### 测试方法论

上面第一个问题暴露的是方法论漏洞：**此前所有用例都显式指定单个 workspace，
从没跑过默认形态** —— 而并发放大只在默认形态下出现。

`tools/live_smoke.py` 新增「CLI 默认形态」段，**跑真命令、用默认参数**，
不再走 API 层捷径；并新增「连续调用 3 次不触发限流」用例
（限流是累积的，单次跑通不代表连续跑通，而 agent 场景下同一命令会被反复调用）。

254 tests passed。

## v0.4.0 - 2026-07-30

平台把 `/api/v1` 逐步下线，本版把 qzcli 的接口层迁到 `/api/v2`，并修掉多 agent 共用开发机时的一批并发问题。**没有 CLI 破坏性变更** —— 所有子命令的用法和输出形状保持不变。

### 平台接口 v1 → v2（v2 优先 + v1 兜底）

官方 CLI `qz` 已是纯 v2 客户端（11 services / 144 actions），而 qzcli 此前 16 个平台端点里只有 2 个在 v2。**v1 衰减不是假设**：`/openapi/v1/specs/list` 已经 404，qzcli 一直在静默回落本地缓存，而它正卡在 `qzcli create` 的关键路径上。

公开方法签名一律不变，内部拆成 `_xxx_v2` / `_xxx_v1` 两条腿，由 `api._v2_then_v1` 分发。已迁：`train ListJobs / GetJob / StopJob / ListJobEvents`、`notebook ListNotebooks`、`workspace ListNodeDimension / ListTaskDimension / GetBasicInfo / GetOverviewTaskMetric`、`hpc ListJobs`。

**回落判据是刻意收紧的**：只有路由不通（404/405/50x、被网关 302 成 HTML）才回落。业务错误（`AccessForbidden`、`InvalidParameter`）直接抛 —— 回落只会把权限问题和我们自己的请求 bug 一起藏起来。

三条只有真机实测才能发现的事：

- **`cluster.*` 对普通账号一律 `AccessForbidden`，必须走 `workspace.*` 双胞胎。** 两者在 `qz spec` 里的描述几乎一模一样，照字面映射会全线踩坑。
- **v2 没有任何 action 返回 `spec_id`**（144 个 action 的 schema 全 grep 过），`node_specs[]` 只有硬件参数没有 id。`list_specs` 改为从历史任务的 `framework_config[].instance_spec_price_info.quota_id` 反推 —— 这是目前唯一能从 v2 拿到真实 spec id 的路径。副作用是 `qzcli res -u` 把 specs 刷空后 `create` 仍然可用（此前会报「无法解析规格」）。
- **`inference-serving` 整个 service 在网关上 404**（spec 声明 18 个 action，路由没挂）。qzcli 不用它，不影响。

**两处仍在 v1**（v2 确实没有对应能力）：`/api/v1/project/list`（v2 的 `ListProjects` 普通账号无权限，且全域没有 `ListWorkspaces`）、`/api/v1/notebook/lab/{id}`（v2 拿不到 Jupyter 访问地址，`qzcli exec` 依赖它）。

### 多 agent 并发 exec：session 隔离

多个 AI agent 常同时对**同一台开发机**跑 `qzcli exec`，此前会互相串数据。修了三层：

- **job_id 撞车（主因）**：原来只有秒级时间戳，同一秒内起的多个 exec 拿到**完全相同的 job_id**，共用一个输出文件互相覆盖。3 路并发实测：第 3 路收到第 2 路的输出、第 1 路什么都没收到。现在 job_id 是 `qzcli_<session>_<ts>_<随机>`。
- **抢终端**：原来复用 `terms[0]`，不只 agent 之间互抢 —— 实测有开发机躺着 4 个别人两天前的人工交互终端，exec 会挑中第一个直接往里发命令。现在每次自建终端、用完删掉，命令用 `setsid`（无则 `nohup`）摘出去以免被带走。
- **文件永久泄漏**：清理只发生在拿到 exit 文件时，`--detach` 后没 attach 的、Ctrl-C 的、超时的全都一直留着。现在输出按 `/tmp/.qzcli/<session>/` 分目录，超过 7 天的旧 session 目录在下次 launch 时自动清理。

新增 `QZCLI_SESSION_ID`（env → `.env` → `config.json` → **按进程自动生成**，与凭据同一套优先级）。不设也不会串车。

新增 **`qzcli exec --list`**：列出开发机上的 exec 任务，默认只列本 session，`--all` 看全部。补上「`--detach` 之后忘了记 job_id 就再也找不回来」的缺口。

老格式 job_id（无 session 段）仍能 `exec-attach`，会回落到平铺路径。

### 修复

- **`qzcli hpc` 此前完全不可用**：平台新增必填 `priority`，不传直接被拒 `priority must be set`。已补 `--priority`。⚠️ **HPC 的优先级方向与训练任务相反** —— 实测提交 1→LOW(11)、3→LOW(13)、5→HIGH(30)、10→HIGH(35)，数字越大越高（有效 1-10），而训练任务的 `task_priority` 是 10 表示低优。默认取 1（LOW），与集群现有生产 HPC 任务一致。
- **`qzcli ws -w <中文名>` 直接崩**（`'latin-1' codec can't encode`）：`cmd_workspace` 拿 `-w` 的值当 workspace_id 用、从不解析名字，中文名原样拼进 referer 头导致请求发不出去。改为复用已有的 `_resolve_workspace_value`。
- **已禁用的工作空间会混进列表**：`project/list` 会把你不是成员的项目也返回，其 `space_list` 里可能挂着已禁用空间（v2 报 `AccessForbidden: 该空间已被禁用`，v1 反而返回陈旧集群结构）。`list_workspaces` 改为按 `usage_status != 0` 滤掉并提示跳过了哪些。
- **MCP 与 CLI 的提交路径分叉**：CLI 早已默认 v2 `CreateJobConsole`，`mcp_server` 还停在 v1，导致 v2 才支持的 `exclude_nodes` 在 MCP 侧静默失效。已统一。
- **`create_job_v2` 漏发 `x-inspire-client-source`**（缺它 APISIX 会把请求 302 到 Keycloak），且没挂 `@with_auth_retry`（提交中途 cookie 过期直接失败）。折进 `_request_v2` 后一并修好。
- 26 处硬编码 `https://qz.sii.edu.cn` 的 origin/referer 改用 `self.base_url` / `api.base_url`，非默认 `QZCLI_API_URL` 下不再发出错配的 Origin。

### 新增工具与文档（进仓，可复现）

| 文件 | 用途 |
|---|---|
| `tools/gen_api_spec_doc.py` | 扫 `qz spec`/`schema`/`--dry-run`，生成全部 144 个 action 的接口文档 |
| `tools/probe_v2.py` | cookie 可用性探针，只打只读 Action，产物自动对 UUID 打码 |
| `tools/compare_v1_v2.py` | v1/v2 逐字段 diff，防「静默返回空」 |
| `tools/live_smoke.py` | 活体冒烟，每个功能点在真实平台跑一遍（`--submit` 含真实提交+停止） |
| `docs/api_spec_v2.json` | 结构化接口定义，**平台改接口后 `git diff` 就能看出变了什么** |
| `docs/v1_to_v2_mapping.md` | 端点映射表（真机实测 + 踩坑 + 平台侧缺口） |
| `docs/v2_probe_report.md` | 109 个只读 Action 的 cookie 可用性结果 |

### 升级说明

- **无需改任何调用方式**，CLI 子命令和输出形状不变。
- `api.list_specs()` 多了一个可选参数 `workspace_id`（历史任务反推需要按工作空间查）。直接调用 `qzcli.api` 的代码不受影响；如果你 mock 或子类化了它，签名要跟着加。
- `QzAPIError` 新增 `api_code` 属性，承载 v2 信封里的 `ResponseMetadata.Error.Code`，用于按结构判断错误类型而不是抠错误文案。
- 多 agent 场景建议显式设 `QZCLI_SESSION_ID`；不设则每进程自动一个。

### 早前已在 Unreleased 中记录的改动

- **`qzcli dashboard` 成分下钻可视化看板 (P1)**: 新增子命令，用 Streamlit + plotly treemap/sunburst 把工作空间的在跑 GPU 占用按「**计算组(机房) → 优先级档 → 项目 → 用户 → 任务**」逐层下钻（块面积 = GPU 数，点块放大、面包屑退回），一眼看清「各计算组里谁占最多、各自高低优」；配色可切优先级/类型/**GPU 利用率**（暴露申请多却空转的任务）。关键在于计算组归属：`list_task_dimension` / `list_node_dimension` 都不直接带 logic_compute_group，改为逐 lcg 用 `list_node_dimension(logic_compute_group_id=…)` 反建「节点→计算组」映射（与 `avail` 同法），再经 `nodes_occupied` 挂到任务，实测 100% 覆盖。共享数据层 `fetch_all_task_dimensions` / `build_node_to_lcg_map` / `task_dimension_to_row` 落在 `cli.py`，`cmd_usage` 复用（输出不变）。下钻层级、视图（treemap/sunburst/icicle）、配色维度均可现场切换：配色支持 优先级/类型/**GPU 利用率**（红=申请多却空转）/**运行时长**（越久越红，按 95 分位截断避免超长任务拉平色阶）。工作空间用**下拉框**选（读本地 `resources.json` 缓存）；顶部有**按任务类型占比**行（交互式建模/训练/推理各占多少 GPU），以及**已占用/空闲 GPU** KPI；勾选「叠加空闲 GPU（灰块）」把各计算组的**剩余容量**（节点 `gpu.total − used` 聚合）以灰块叠加。悬停任意块给出干净明细卡片（任务数/类型/GPU 加权平均利用率/最长运行时长），对内部节点也做了逐层聚合（不再是 px 默认的 `NaN`/`(?)`）。任务分页与逐 lcg 节点查询用 `ThreadPoolExecutor` **并发拉取**（分布式空间首屏 ~17s → ~5s），`cmd_usage` 同样受益。看板依赖走可选 extra：`pip install 'qzcli[dashboard]'`。
- **Cookie 过期自动重登 (P0)**: 所有 cookie 认证的 API 方法（`*_with_cookie`、`_request_v2`、HPC/节点维度查询等）现在用 `@with_auth_retry` 装饰——遇到 401 会用本地凭据透明地 `login_with_cas` 重登一次并重试，消除了此前在长会话 / 自动化中每隔 ~20 分钟手动 `qzcli login` 的反复操作。无凭据或重登失败时回退到原有行为（如 token 认证）。`exec` 取 Jupyter 连接信息时同样会在 cookie 过期时自动重登。
- **CAS 登录重试退避 (P1)**: `login_with_cas` 现在对瞬时故障（SSL `UNEXPECTED_EOF_WHILE_READING`、连接重置、CAS/代理 5xx）做指数退避重试（最多 3 次）；用户名密码错误等永久性错误立即抛出、不重试。新增 `QzTransientError`（`QzAPIError` 子类）用类型而非文案标记可重试错误。
- **`exec` 分离式后台执行 (P1)**: `qzcli exec --detach`（别名 `--no-wait`）后台启动命令并立即返回 `job_id`；`qzcli exec-attach <target> <job_id>` 重连并继续拉取输出。`exec` 超时不再丢弃输出，而是保留远端文件并打印可直接复制的 `exec-attach` 续读命令。底层 `_exec_via_jupyter` 拆分为 `_exec_launch` / `_exec_poll`。
- **新增 MCP 工具 `qz_exec` / `qz_exec_attach`**: agent 无需 shell-out 即可在开发机执行命令；`detach=True` 用于编译、下载、训练等长命令，配合 `qz_exec_attach` 轮询结果。两者共享上面的 cookie 自动重登。
- **`exec` / `exec-attach` 的 target 支持 notebook_id 及其前缀**: 除了名字、完整 UUID、URL，现在也能直接粘贴 notebook_id 或它的一段前缀；`_resolve_notebook_id_by_name` 先按 name/notebook_id 精确命中，未命中再按 notebook_id 前缀模糊匹配——前缀唯一才解析，撞到多个时列出候选并报错（不默默取第一个）。CLI 与 MCP `qz_exec` 共用此解析路径。

## v0.3.0 - 2026-05-28

Second tagged release. Two breaking changes around resource refresh and the job-creation payload, plus exec polish and several reliability fixes since v0.2.0.

### Highlights

- **`qzcli exec` now production-ready for pasted targets**: accepts dev-machine name, notebook UUID, **or** a full IDE / Jupyter URL pasted from the browser, with new `--timeout` flag and a user_ids-filter fix that previously hid other people's dev machines.
- **Much faster `qzcli res -u`**: default-flip to quick mode + parallel workspace refresh drops a full cache refresh on 19 workspaces from "hangs indefinitely" to under a minute.
- **`qzcli create` migrated to the new `resource_spec_price` schema**: the platform rejected the legacy `framework_config[0].spec_id` field with `unknown field "spec_id"`; new payload also promotes cpu / mem_gi / gpu_count to satisfy the platform's `Cpu and Mem can't be empty.` check.

### Breaking changes

- `qzcli res -u` now defaults to **quick mode** — skips the unbounded historical-jobs walk and pulls `compute_groups` / `projects` directly from `get_cluster_basic_info` / `list_task_dimension`. `specs` are no longer refreshed in the default path. Pass `--full` / `-F` to opt back into the legacy full scan (still required when you need fresh spec ids for `cmd_create` non-interactive). The `--quick` / `-q` flag is preserved as a no-op for backward compatibility with existing scripts. `create -i` still uses the full scan internally because submission needs spec ids.
- `qzcli create` and the `qz_create_job` MCP tool now build the `/api/v1/train_job/create` payload using the platform's new `resource_spec_price` schema; the legacy `framework_config[0].spec_id` field is no longer sent. If you were assembling payloads by hand against an older platform build, regenerate them.

### What's new

- `qzcli exec`: target argument now accepts dev-machine name, notebook UUID, **or** a full IDE / Jupyter URL pasted from the browser (`/ide?notebook_id=...`, `/jobs/interactiveModel(ing)?Detail/...`, `/jupyter/...`, `/api/v1/notebook/lab/...`, `/notebook/(lab|code)/...`); add `--timeout` flag (default 120s, must precede `target` because `remote_cmd` uses `argparse.REMAINDER`); drop the `user_ids` filter on the underlying `list_notebooks` call so name resolution no longer hides dev machines whose `created_by` differs from the caller. See the new "远程执行 / 开发机命令" section in README for usage. (aad08d0, #29, #30)
- `qzcli res -u` now refreshes workspaces **in parallel** via `ThreadPoolExecutor`. Default `--parallel 8`; pass `--parallel 1` to recover the old sequential behavior (useful when debugging or when a workspace's API is misbehaving). Disk writes (`save_resources`) still run on the main thread as results land, so there's no read-modify-write race on `~/.qzcli/resources.json`. Combined with the default-quick flip, full `res -u` on 19 workspaces drops from "hangs indefinitely" to under a minute end-to-end on this machine.
- `qzcli res -u` now shows a live progress bar during workspace cache refresh, sharing the rich `display.create_progress()` pattern from `qzcli avail`.
- Reapply most of PR #23 (cookie auth for `get_job_detail` / `stop_job`, `qz_get_hpc_usage` unpacking fix, `qz_track_job` error propagation). The `_get_token` encrypt-password change is intentionally *not* reapplied — the `{"encrypted": True}` flag is not part of `/auth/token`'s documented contract and likely doesn't help CAS-federated users with `invalid_grant` anyway. Issue #14 remains open.
- `qzcli create` now prefers cookie auth (`/api/v1/train_job/create`) by default and only falls back to the openapi token path when no cookie is available — this aligns the CLI with `qz_create_job` and unblocks CAS-federated users who previously got `invalid_grant`.
- New public helper `qzcli.api.build_resource_spec_price(spec_obj, compute_group_id)` shared by CLI and MCP. New CLI helper `_lookup_spec_for_payload` auto-refreshes the spec cache when cpu/gpu/memory fields are missing, and gives a `qzcli res -u` hint if still unresolved.
- Documentation: README now documents `qzcli exec` in the 任务管理 table and a new 远程执行 / 开发机命令 subsection (was completely missing despite shipping in v0.2.0). (#30)

### Upgrade notes

- No Python version change (`>=3.8`).
- If you rely on `qzcli res -u` populating `specs`, add `--full` to your refresh script.
- If you assemble `/api/v1/train_job/create` payloads yourself, switch to the new `resource_spec_price` schema; `framework_config[0].spec_id` is now rejected by the platform.

### Validation

- `python3 -m compileall qzcli tests`
- `python3 -m unittest discover -s tests`

## v0.2.0 - 2026-05-04

qzcli v0.2.0 is the first tagged release for the project, collecting the recent work on job logs, WSL/VPN networking, faster capacity checks, MCP integration, interactive workloads, and HPC/CPU job submission into a versioned release.

## Highlights

- WSL/VPN Proxy Support: Add config-driven SOCKS and HTTP(S) proxy routing across OpenAPI calls, cookie-authenticated APIs, `/api/v2` calls, and CAS login, including explicit handling for WSL/Clash setups where `HTTPS_PROXY=http://...` can override a SOCKS proxy, plus a declared `PySocks` dependency for reliable installation: #13
- Fast Availability Queries: Preserve cached fuzzy workspace matching, avoid slow full workspace scans for targeted `qzcli avail -w` queries, parallelize node/task dimension fetches, increase low-priority task pagination, add progress display, and reduce measured local `qzcli avail` runtime from about 49.7s to about 5.9s in the optimized path: #17
- Job Logs CLI: Add `qzcli logs <job-id>` backed by `/api/v2/train?Action=GetJobLog` with cookie authentication, chronological tail output, `--tail`, `--follow`, `--raw`, `--json`, `--pod`, and `--since` support, plus documentation and clearer login failure hints: #16, 0e66dc4, 0d6c706, 16fed90
- Job Creation and MCP Workflows: Add `qzcli create`, `qzcli batch`, MCP job creation tools, cookie-based job creation fallback, optional MCP login credential resolution, and interactive job submission with auto-login support: #7, #12, #13
- Interactive and HPC Workloads: Add dev-machine listing, Jupyter-terminal based remote command execution, HPC/CPU job submission, HPC command documentation, and HPC CPU/memory utilization in availability output: #9, #10, 93e40a6, fa8cc23
- Login and Workspace Reliability: Fix CAS RSA ciphertext encoding when leading zero bytes are present, handle passwords with special characters, move workspace overview to the new platform endpoint, and keep backward-compatible workspace flags as deprecated no-ops: #8, #11, 4cf71ce, ed10a5f

## Upgrade Notes

- No known breaking changes; Python support remains `>=3.8`.
- SOCKS proxy users should install updated dependencies with `pip install -r requirements.txt`.
- WSL users can configure a Windows-side Clash proxy with `{"proxy": "socks5://127.0.0.1:7897"}` in `~/.qzcli/config.json`.
- `qzcli logs` and other `/api/v2` calls require cookie authentication; run `qzcli login` again if a command reports an expired or missing cookie.

## Dependencies

- Add `PySocks>=1.7` so SOCKS proxy support works after a normal requirements install: #13

## Validation

- `python3 -m compileall qzcli tests`
- `python3 -m unittest discover -s tests` (54 passed, 1 skipped)

## What's Changed

- Add sii-cas-auth auto login password encryption: #1
- Unify `avail` table output and improve formatting: #2
- Add qzcli MCP server support: #6
- Add `qzcli create` and `qzcli batch` commands for job submission: #7
- Preserve leading zero in RSA ciphertext: #8
- Add dev machine listing and Jupyter-based remote exec: #9
- Add HPC/CPU job submission support: #10
- Update workspace overview to use the new API endpoint: #11
- Support interactive job submitting and auto-login: #12
- Support SOCKS5 proxy for WSL/VPN environments: #13
- Document `qzcli logs`: #16
- Preserve cached workspace matches and speed up `qzcli avail`: #17

Full Changelog: https://github.com/tianyilt/qzcli_tool/commits/v0.2.0
