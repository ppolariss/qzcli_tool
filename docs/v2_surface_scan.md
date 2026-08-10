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

## 只读 action 探活

写操作一律跳过（见模块 docstring 的安全边界）。
`需参数` / `权限` **都证明路由存在**，只有 `不存在` 是真没有。

| 服务 | Action | 结果 | 详情 |
|---|---|---|---|
| `audit` | `GetAuditDetail` | 需参数 | AuditId is required |
| `audit` | `GetAuditList` | 可用 | 返回 ['Result'] |
| `audit` | `GetSecurityShareList` | 可用 | 返回 ['Result'] |
| `billing` | `GetProjectBillingDetail` | 异常 | ReadTimeoutError: HTTPSConnectionPool(host='qz.sii.edu.cn', port=443): Read timed out. (read timeout=60.0) |
| `billing` | `ListProjectBillings` | InternalError | internal server error |
| `billing` | `ListUserBillings` | InternalError | internal server error |
| `cluster` | `GetClusterBasicInfo` | 权限 | Access denied |
| `cluster` | `GetClusterNodeDistincts` | 可用 | 返回 ['Result'] |
| `cluster` | `GetClusterNodes` | 可用 | 返回 ['Result'] |
| `cluster` | `GetClusterOverviewOptions` | 权限 | Access denied |
| `cluster` | `GetComputeGroupById` | 需参数 | ComputeGroupId is required |
| `cluster` | `GetConsumeTimeSeries` | 需参数 | 系统错误 |
| `cluster` | `GetNodeDistincts` | 可用 | 返回 ['Result'] |
| `cluster` | `GetNodeResourceInfoDictionary` | 可用 | 返回 ['Result'] |
| `cluster` | `GetNodeResourceTypeById` | 需参数 | NodeResourceTypeId is required |
| `cluster` | `GetOverviewResourceMetric` | 权限 | Access denied |
| `cluster` | `GetOverviewResourceMetricByTime` | 权限 | Access denied |
| `cluster` | `GetOverviewTaskMetric` | 权限 | Access denied |
| `cluster` | `GetResourceConsumeTimeSeries` | 需参数 | 系统错误 |
| `cluster` | `GetTaskMetricBatch` | 需参数 | 系统错误 |
| `cluster` | `GetWorkspaceNodes` | 需参数 | WorkspaceId is required |
| `cluster` | `ListClusterRegions` | 权限 | Access denied |
| `cluster` | `ListClusters` | 权限 | Access denied |
| `cluster` | `ListComputeGroups` | 可用 | 返回 ['Result'] |
| `cluster` | `ListLogicComputeGroups` | 权限 | Access denied |
| `cluster` | `ListNodeDimension` | 权限 | Access denied |
| `cluster` | `ListNodeEvents` | 可用 | 返回 ['Result'] |
| `cluster` | `ListNodeResourceType` | InternalError | internal server error |
| `cluster` | `ListNodes` | 可用 | 返回 ['Result'] |
| `cluster` | `ListProjectDimension` | 权限 | Access denied |
| `cluster` | `ListResourceConsumeStats` | InternalError | ProjectId is required |
| `cluster` | `ListTaskDimension` | 权限 | Access denied |
| `cluster` | `ListUserDimension` | 权限 | Access denied |
| `file` | `CheckPermission` | 可用 | 返回 ['Result'] |
| `file` | `GetDirList` | 需参数 | request is nil |
| `file` | `GetSftpgoConnectionInfo` | ResourceNotFound | failed to get storage: record not found |
| `file` | `GetSystemStorageTypeList` | InternalError | invalid request type |
| `file` | `ListFileCopyTasks` | 可用 | 返回 ['Result'] |
| `hpc` | `GetHpcConfigByWorkspaceId` | 需参数 | WorkspaceId is required |
| `hpc` | `GetJob` | 需参数 | JobId is required |
| `hpc` | `GetJobLog` | InternalError | internal server error |
| `hpc` | `GetTaskMetric` | ResourceNotFound | record not found |
| `hpc` | `GetTaskMetricBatch` | 需参数 | 系统错误 |
| `hpc` | `GetUserProjectWorkingDir` | ResourceNotFound | record not found |
| `hpc` | `ListJobEvents` | InternalError | internal server error |
| `hpc` | `ListJobInstances` | 需参数 | JobId is required |
| `hpc` | `ListJobs` | 需参数 | 参数错误 |
| `hpc` | `ListSlurmdPodEvent` | 可用 | 返回 ['Result'] |
| `image` | `GetImageById` | 需参数 | ImageId is required |
| `image` | `ListImageBrands` | 可用 | 返回 ['Result'] |
| `image` | `ListImages` | 可用 | 返回 ['Result'] |
| `inference-serving` | `GetInferenceServingTerms` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=GetInferenceServingTerms 这条路由（404）。 |
| `inference-serving` | `GetInferenceServingUserProjectList` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=GetInferenceServingUserProjectList 这条路由（404）。 |
| `inference-serving` | `GetLastSuccessInferenceServingInfo` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=GetLastSuccessInferenceServingInfo 这条路由（404）。 |
| `inference-serving` | `GetServing` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=GetServing 这条路由（404）。 |
| `inference-serving` | `GetServingApiMetric` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=GetServingApiMetric 这条路由（404）。 |
| `inference-serving` | `GetServingApiMetricBatch` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=GetServingApiMetricBatch 这条路由（404）。 |
| `inference-serving` | `GetServingConfigByWorkspaceId` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=GetServingConfigByWorkspaceId 这条路由（404）。 |
| `inference-serving` | `GetServingLog` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=GetServingLog 这条路由（404）。 |
| `inference-serving` | `GetTaskMetric` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=GetTaskMetric 这条路由（404）。 |
| `inference-serving` | `GetTaskMetricBatch` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=GetTaskMetricBatch 这条路由（404）。 |
| `inference-serving` | `ListServingEvents` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=ListServingEvents 这条路由（404）。 |
| `inference-serving` | `ListServingInstances` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=ListServingInstances 这条路由（404）。 |
| `inference-serving` | `ListServingVersions` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=ListServingVersions 这条路由（404）。 |
| `inference-serving` | `ListServings` | 异常 | QzAPIError: v2 网关上没有 /api/v2/inference-serving?Action=ListServings 这条路由（404）。 |
| `job` | `ListJobs` | 需参数 | 参数错误 |
| `model-hub` | `CheckModelVLLMCompatible` | 需参数 | ModelId is required |
| `model-hub` | `GetAllVersionModelsById` | 需参数 | ModelId is required |
| `model-hub` | `GetHasModelPendingServing` | 可用 | 返回 ['Result'] |
| `model-hub` | `GetModelCreators` | 可用 | 返回 ['Result'] |
| `model-hub` | `GetModelDetail` | ResourceNotFound | record not found |
| `model-hub` | `GetModelDetails` | ResourceNotFound | record not found |
| `model-hub` | `GetModelRelatedServings` | 可用 | 返回 ['Result'] |
| `model-hub` | `GetModelVLLMCompatibleData` | 需参数 | ModelId is required |
| `model-hub` | `GetModelVersionList` | 需参数 | ModelId is required |
| `model-hub` | `GetRecommendedConfig` | 需参数 | ModelId is required |
| `model-hub` | `ListModel` | ResourceNotFound | compute group ids not found |
| `model-hub` | `ListModelCreators` | 可用 | 返回 ['Result'] |
| `model-hub` | `ListModelRelatedServings` | 可用 | 返回 ['Result'] |
| `model-hub` | `ListModelVersionOptions` | 需参数 | ModelId is required |
| `model-hub` | `ListModelVersions` | 需参数 | ModelId is required |
| `model-hub` | `ListModels` | ResourceNotFound | compute group ids not found |
| `model-hub` | `SearchModelTags` | 可用 | 返回 ['Result'] |
| `notebook` | `CheckNotebook` | 可用 | 返回 [] |
| `notebook` | `GetMyInspireCodeNotebookList` | 可用 | 返回 ['Result'] |
| `notebook` | `GetNotebook` | 需参数 | NotebookId is required |
| `notebook` | `GetNotebookAccessUrl` | 需参数 | NotebookId is required |
| `notebook` | `GetRealtimeNotebookMetric` | 可用 | 返回 ['Result'] |
| `notebook` | `GetRealtimeNotebookMetricByTime` | 可用 | 返回 ['Result'] |
| `notebook` | `GetScheduleConfig` | 需参数 | WorkspaceId is required |
| `notebook` | `GetTaskMetric` | 需参数 | 系统错误 |
| `notebook` | `GetTaskMetricBatch` | 需参数 | 系统错误 |
| `notebook` | `ListNotebookCreators` | 需参数 | WorkspaceId is required |
| `notebook` | `ListNotebookEvents` | 需参数 | NotebookId is required |
| `notebook` | `ListNotebookLifecycles` | 可用 | 返回 ['Result'] |
| `notebook` | `ListNotebooks` | 需参数 | 参数错误 |
| `notebook` | `ListRunIndex` | 需参数 | NotebookId is required |
| `project` | `CheckName` | 可用 | 返回 ['Result'] |
| `project` | `CheckProjectMemberResourceLimit` | 需参数 | ProjectId is required |
| `project` | `GetProjectBudgetUsageOverview` | 需参数 | ProjectId is required |
| `project` | `GetProjectDetail` | 需参数 | ProjectId is required |
| `project` | `GetProjectForPage` | 可用 | 返回 ['Result'] |
| `project` | `GetProjectListV2` | 需参数 | WorkspaceId is required |
| `project` | `GetProjectMemberBudgetConfig` | 需参数 | ProjectId is required |
| `project` | `GetProjectMemberBudgetUsage` | 需参数 | ProjectId is required |
| `project` | `GetProjectMemberList` | 需参数 | ProjectId is required |
| `project` | `GetProjectMemberResourceConfig` | 需参数 | ProjectId is required |
| `project` | `GetProjectOwners` | 可用 | 返回 ['Result'] |
| `project` | `GetProjectResourceHistory` | 需参数 | ProjectId is required |
| `project` | `GetSubProjectDetail` | 需参数 | ProjectId is required |
| `project` | `GetSubProjectForPage` | 需参数 | ProjectId is required |
| `project` | `GetWebhookList` | 需参数 | ProjectId is required |
| `project` | `ListMountProjects` | 权限 | Access denied |
| `project` | `ListProjectRunningTasks` | 权限 | Access denied |
| `ray` | `GetJob` | 需参数 | RayJobId is required |
| `ray` | `GetJobLog` | InternalError | internal server error |
| `ray` | `GetTaskMetric` | 需参数 | 系统错误 |
| `ray` | `GetTaskMetricBatch` | 需参数 | 系统错误 |
| `ray` | `ListJobCreators` | 可用 | 返回 ['Result'] |
| `ray` | `ListJobEvents` | 可用 | 返回 ['Result'] |
| `ray` | `ListJobInstances` | 可用 | 返回 ['Result'] |
| `ray` | `ListJobScalingHistories` | 可用 | 返回 ['Result'] |
| `ray` | `ListJobs` | 需参数 | 参数错误 |
| `resource-price` | `GetGpuResourceNameInfos` | 可用 | 返回 ['Result'] |
| `resource-price` | `GetLogicComputeGroupResourceSpecPrices` | 需参数 | unspecified schedule config type |
| `resource-price` | `GetResourceAndInferencePrices` | 可用 | 返回 ['Result'] |
| `resource-price` | `GetStoragePrices` | 可用 | 返回 ['Result'] |
| `sandbox` | `GetSandbox` | 需参数 | SandboxId is required |
| `sandbox` | `GetSandboxConfig` | 可用 | 返回 ['Result'] |
| `sandbox` | `ListSandboxes` | 需参数 | 参数错误: workspace_id is required |
| `sandbox-api-key` | `ListSandboxAPIKeys` | 需参数 | workspace_id is required |
| `sandbox-pool` | `GetSandboxPoolByID` | 需参数 | SandboxPoolId is required |
| `sandbox-template` | `GetSbxSpec` | 可用 | 返回 ['Result'] |
| `sandbox-template` | `GetTemplateBuildStatus` | 需参数 | TemplateId is required |
| `sandbox-template` | `ListSandboxTemplates` | 可用 | 返回 ['Result'] |
| `sandbox-template` | `ListTemplateBuilds` | 需参数 | TemplateId is required |
| `storage` | `GetPersonalStorageQuota` | 权限 | user is not system admin |
| `storage` | `GetStgConfig` | 权限 | user is not system admin |
| `storage` | `GetStorageQuota` | 权限 | user is not system admin |
| `storage` | `GetStorageQuotaOverview` | 权限 | user is not system admin |
| `storage` | `GetStorageUsage` | 权限 | user is not system admin |
| `train` | `GetJob` | 需参数 | JobId is required |
| `train` | `GetJobLog` | InternalError | internal server error |
| `train` | `GetJobWorkdir` | 需参数 | WorkspaceId is required |
| `train` | `GetMegatraceResult` | 需参数 | JobId is required |
| `train` | `GetPreCheckResult` | 需参数 | JobId is required |
| `train` | `GetTaskMetric` | 需参数 | 系统错误 |
| `train` | `GetTaskMetricBatch` | 需参数 | 系统错误 |
| `train` | `GetTensorboard` | 需参数 | 用户不存在。 |
| `train` | `ListJobEvents` | 可用 | 返回 ['Result'] |
| `train` | `ListJobIds` | 可用 | 返回 ['Result'] |
| `train` | `ListJobInstanceEvents` | 需参数 | 参数错误 |
| `train` | `ListJobInstances` | 需参数 | JobId is required |
| `train` | `ListJobs` | 需参数 | page or page_size too large |
| `train` | `ListPreCheckItems` | 需参数 | LogicComputeGroupId is required |
| `train` | `ListTensorboardUsers` | 可用 | 返回 ['Result'] |
| `train` | `ListTensorboards` | 可用 | 返回 ['Result'] |
| `user` | `GetAPIKeyPlaintext` | 需参数 | ApiKeyId is required |
| `user` | `GetMyAPIList` | 可用 | 返回 ['Result'] |
| `user` | `GetMyPermissions` | 可用 | 返回 ['Result'] |
| `user` | `GetPermissions` | 需参数 | WorkspaceId is required |
| `user` | `GetRoutes` | 需参数 | WorkspaceId is required |
| `user` | `GetSSHDetailByID` | 需参数 | SshId is required |
| `user` | `GetUserAkList` | 可用 | 返回 ['Result'] |
| `user` | `GetUserDetail` | 可用 | 返回 ['Result'] |
| `user` | `ListAPIKeys` | 可用 | 返回 ['Result'] |
| `user` | `ListSSH` | 可用 | 返回 ['Result'] |
| `user` | `ListUsers` | 权限 | user is not system admin |
| `workspace` | `CheckWorkspaceCanChangeType` | 需参数 | WorkspaceId is required |
| `workspace` | `GetAllQuota` | 权限 | Access denied |
| `workspace` | `GetBasicInfo` | 权限 | Access denied |
| `workspace` | `GetDefaultUserQuota` | 权限 | Access denied |
| `workspace` | `GetDefaultUserTaskQuota` | 需参数 | workspace_id is required |
| `workspace` | `GetLogicComputeGroupById` | 需参数 | LogicComputeGroupId is required |
| `workspace` | `GetLogicComputeGroupNodeSpecs` | 权限 | Access denied |
| `workspace` | `GetLogicComputeGroupResource` | 权限 | Access denied |
| `workspace` | `GetOverviewOptions` | 权限 | Access denied |
| `workspace` | `GetOverviewResourceMetric` | 权限 | Access denied |
| `workspace` | `GetOverviewResourceMetricByTime` | 权限 | Access denied |
| `workspace` | `GetOverviewTaskMetric` | 权限 | Access denied |
| `workspace` | `GetProjectUserQuotaProjectList` | 权限 | Access denied |
| `workspace` | `GetScheduleConfig` | 权限 | Access denied |
| `workspace` | `GetUnbindWorkspaceList` | 可用 | 返回 ['Result'] |
| `workspace` | `GetUserTaskQuota` | 需参数 | workspace_id is required |
| `workspace` | `GetWorkspaceComputeResource` | 权限 | Access denied |
| `workspace` | `GetWorkspaceNodeSpecs` | 权限 | Access denied |
| `workspace` | `GetWorkspaceQuota` | 权限 | Access denied |
| `workspace` | `GetWorkspaceStorageVolume` | 可用 | 返回 ['Result'] |
| `workspace` | `GetWorkspaceTaskQuota` | 需参数 | workspace_id is required |
| `workspace` | `ListConfiguredParentProjectSandboxQuotaProjects` | 需参数 | workspace_id is required |
| `workspace` | `ListConfiguredParentProjectUserSandboxQuotaUsers` | 需参数 | workspace_id is required |
| `workspace` | `ListConfiguredSubProjectUserSandboxQuotaUsers` | 需参数 | workspace_id is required |
| `workspace` | `ListConfiguredWorkspaceUserSandboxQuotaUsers` | 需参数 | workspace_id is required |
| `workspace` | `ListLogicComputeGroups` | 权限 | Access denied |
| `workspace` | `ListManagedProjects` | 权限 | Access denied |
| `workspace` | `ListNodeDimension` | 权限 | Access denied |
| `workspace` | `ListProjectDimension` | 权限 | Access denied |
| `workspace` | `ListProjectQuotas` | 权限 | Access denied |
| `workspace` | `ListTaskDimension` | 权限 | Access denied |
| `workspace` | `ListUserDimension` | 权限 | Access denied |
| `workspace` | `ListUserQuotas` | 权限 | Access denied |
| `workspace` | `ListWorkspaceMembers` | 权限 | Access denied |
| `workspace` | `ListWorkspaceNodes` | 权限 | Access denied |
| `workspace` | `ListWorkspaceParentProjects` | 权限 | Access denied |
| `workspace` | `ListWorkspaces` | 可用 | 返回 ['Result'] |

### 汇总

- 跳过(写操作): 103
- 需参数: 78
- 可用: 51
- 权限: 45
- 异常: 15
- InternalError: 9
- ResourceNotFound: 7
