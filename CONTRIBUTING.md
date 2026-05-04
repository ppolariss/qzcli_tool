# Contributing to qzcli

感谢你愿意参与 qzcli。这个项目主要围绕启智平台的真实任务管理工作流演进，欢迎提交 bug report、接口分析、文档改进和 PR。

## 开发环境

```bash
git clone https://github.com/tianyilt/qzcli_tool.git
cd qzcli_tool
python -m pip install -r requirements.txt
python -m pip install -e .
```

## 本地验证

提交 PR 前至少运行：

```bash
python3 -m compileall qzcli tests
python3 -m unittest discover -s tests
git diff --check
```

如果改动涉及真实平台接口，请在 PR 里说明：

- 运行过的 qzcli 命令
- 是否使用 cookie auth 或 token auth
- 是否涉及 workspace / project / compute group / spec 解析
- 是否补了单元测试或手工验证步骤

## PR 建议

- 小步提交，单个 PR 尽量只解决一个问题。
- 新增命令或参数时同步更新 `README.md`。
- 修改认证、任务提交、资源查询等共享路径时补测试。
- 避免把真实 cookie、用户名、workspace UUID、内部项目名、完整日志贴进仓库；必要时请脱敏。

## Issue 建议

提 bug 时请尽量包含：

- qzcli 版本或 commit
- 运行命令
- 期望行为和实际行为
- 脱敏后的错误输出
- 是否已经执行过 `qzcli login` / `qzcli res -u`

功能建议可以直接描述具体使用场景，例如“提交任务前想自动找满足 N 节点的 compute group”。
