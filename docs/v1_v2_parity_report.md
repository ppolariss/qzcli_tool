# v1 / v2 全量对齐扫描报告

- 扫描工作空间：**16**
- 扫描端点：projects, jobs, notebooks, nodes, tasks, basic_info, overview
- 发现差异：**4**（SCHEMA 2 / VALUE 2 / VOLUME 0 / ERROR 0）

> **判据分档**（混在一起看会淹没真问题）：
> - `SCHEMA` 字段名不一致 —— **最危险**。v2 换字段名不会报错，只会静默返回空，代码照跑
> - `VALUE`  同记录同字段值不同 —— 要人判断是真差异还是实时波动
> - `VOLUME` 条目数/total 不一致 —— 分页或过滤语义不同
> - `ERROR`  只有一边报错 —— 通常是 v2 权限更严或路由缺失

> 已核实无害、不再计入的差异：
> - `brand_name` —— v2 补充中文品牌名（v1 为空串），代码不读该字段

## SCHEMA（2）

| 端点 | 工作空间 | 详情 |
|---|---|---|
| `projects` | （全局） | 元素字段不同：只在 v1=['member_remain_budget'] 只在 v2=[] |
| `jobs` | 分布式训练空间 | 元素字段不同：只在 v1=['running_time_ms'] 只在 v2=[] |

## VALUE（2）

| 端点 | 工作空间 | 详情 |
|---|---|---|
| `projects` | （全局） | 10 条交集里字段值不一致：is_member×10, remain_budget×10, sub_project_list×1 |
| `jobs` | 分布式训练空间 | 20 条交集里字段值不一致：node_count×1, node_infos×1, timeline×1 |
