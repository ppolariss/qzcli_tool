# 平台 v2 真实接口面扫描

由 `tools/scan_v2_surface.py` 生成。**前端列是下界**（只统计写死的调用），
所以本表能证明「某 action 存在」，不能证明「某 action 不存在」。

## 服务级对账

| 服务 | spec | 前端 | spec 缺 |
|---|---:|---:|---|
| `audit` | 0 | 6 | **6** |
| `billing` | 0 | 3 | **3** |
| `cluster` | 27 | 7 | **2** |
| `file` | 0 | 9 | **9** |
| `hpc` | 13 | 0 | — |
| `image` | 4 | 1 | — |
| `inference-serving` | 23 | 0 | — |
| `job` | 0 | 1 | **1** |
| `model-hub` | 13 | 16 | **12** |
| `notebook` | 21 | 2 | **2** |
| `operate-log` | 0 | 1 | **1** |
| `project` | 1 | 32 | **31** |
| `ray` | 14 | 6 | — |
| `resource-price` | 0 | 6 | **6** |
| `sandbox` | 0 | 4 | **4** |
| `sandbox-api-key` | 0 | 3 | **3** |
| `sandbox-pool` | 0 | 1 | **1** |
| `sandbox-template` | 0 | 6 | **6** |
| `serving` | 0 | 1 | **1** |
| `storage` | 0 | 10 | **10** |
| `train` | 19 | 10 | **5** |
| `user` | 3 | 16 | **14** |
| `workspace` | 31 | 28 | **22** |

合计：spec 169 个 action / 11 服务；前端 169 个 / 21 服务。

