from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import json as _json
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import requests

from . import __version__
from .config import (
    clear_token_cache,
    get_api_base_url,
    get_cookie,
    get_credentials,
    get_proxy,
    get_token_cache,
    save_cookie,
    save_token_cache,
)
from .crypto import encrypt_password


# /api/v2/* requires this APISIX header; without it the gateway can return an
# HTML login page even when the CAS cookie is otherwise valid.
V2_CLIENT_SOURCE = f"qzcli/{__version__}"
V2_BROWSER_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)


class QzAPIError(Exception):
    """API 错误"""
    def __init__(self, message: str, code: Optional[int] = None):
        super().__init__(message)
        self.code = code


@lru_cache(maxsize=8)
def _get_pool_manager(proxy: str):
    """Return a cached urllib3 manager for the configured proxy URL."""
    import urllib3

    if not proxy:
        return urllib3.PoolManager()

    normalized = proxy.rstrip("/") + "/"
    if normalized.lower().startswith(
        ("socks4://", "socks4a://", "socks5://", "socks5h://")
    ):
        try:
            from urllib3.contrib.socks import SOCKSProxyManager
        except ImportError as exc:
            raise QzAPIError(
                "当前代理配置需要 SOCKS 支持，请安装 PySocks 或 urllib3[socks]",
            ) from exc
        return SOCKSProxyManager(normalized)

    if normalized.lower().startswith(("http://", "https://")):
        return urllib3.ProxyManager(normalized)

    raise QzAPIError(f"不支持的代理地址: {proxy}")


class _CurlResponse:
    """Minimal response object mimicking requests.Response."""

    def __init__(
        self,
        status_code: int,
        text: str,
        url: str = "",
        headers: Optional[Dict[str, str]] = None,
    ):
        self.status_code = status_code
        self.text = text
        self.url = url
        self.headers = headers or {}

    def json(self):
        return _json.loads(self.text)


def _curl_post(
    url: str,
    *,
    json: Any = None,
    headers: Optional[Dict[str, str]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 60,
    **_kw,
) -> _CurlResponse:
    """Drop-in replacement for requests.post with explicit proxy handling."""
    if params:
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{urlencode(params)}"

    pm = _get_pool_manager((get_proxy() or "").strip())
    body = _json.dumps(json).encode("utf-8") if json is not None else None
    hdrs = dict(headers) if headers else {}
    header_names = {name.lower() for name in hdrs}
    if json is not None and "content-type" not in header_names:
        hdrs["Content-Type"] = "application/json"
    resp = pm.request(
        "POST",
        url,
        body=body,
        headers=hdrs,
        timeout=float(timeout),
        redirect=False,
    )
    return _CurlResponse(
        status_code=resp.status,
        text=resp.data.decode("utf-8", errors="replace"),
        url=url,
        headers=dict(resp.headers),
    )


class QzAPI:
    """启智平台 API 客户端"""
    
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None):
        self.base_url = get_api_base_url()
        
        if username and password:
            self._username = username
            self._password = password
        else:
            self._username, self._password = get_credentials()
        
        self._token: Optional[str] = None

    def _resolve_credentials(self) -> tuple[str, str]:
        """Return credentials from explicit args first, then env/config fallback."""
        if self._username and self._password:
            return self._username, self._password

        username, password = get_credentials()
        if username and password:
            self._username = username
            self._password = password

        return self._username or "", self._password or ""

    def ensure_cookie(self, force_refresh: bool = False) -> Dict[str, Any]:
        """Return a usable browser cookie, refreshing via CAS when needed."""
        cookie_data = get_cookie() or {}
        if not force_refresh and cookie_data.get("cookie"):
            return cookie_data

        username, password = self._resolve_credentials()
        if not username or not password:
            raise QzAPIError(
                "Cookie 不可用，且未配置认证信息；请设置 QZCLI_USERNAME/QZCLI_PASSWORD 或运行 qzcli init"
            )

        workspace_id = str(cookie_data.get("workspace_id", ""))
        cookie = self.login_with_cas(username, password)
        save_cookie(cookie, workspace_id=workspace_id)
        return get_cookie() or {"cookie": cookie, "workspace_id": workspace_id}

    @staticmethod
    def _browser_headers(cookie: str, referer: str) -> Dict[str, str]:
        """构造内部 cookie API 通用请求头。"""
        return {
            "accept": "application/json, text/plain, */*",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": "https://qz.sii.edu.cn",
            "pragma": "no-cache",
            "referer": referer,
            "sec-ch-ua": '"Not(A:Brand";v="8", "Chromium";v="144"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
        }

    @staticmethod
    def _parse_json_response(response: requests.Response, invalid_json_message: str) -> Dict[str, Any]:
        try:
            return response.json()
        except Exception as exc:
            raise QzAPIError(invalid_json_message) from exc

    @staticmethod
    def _raise_for_bad_status(response: requests.Response, auth_message: str) -> None:
        if response.status_code == 401:
            raise QzAPIError(auth_message, 401)
        if response.status_code != 200:
            raise QzAPIError(f"请求失败: HTTP {response.status_code}", response.status_code)

    def _post(self, url: str, **kwargs) -> _CurlResponse:
        return _curl_post(url, **kwargs)

    def _post_json(self, url: str, payload: Dict[str, Any], headers: Dict[str, str], timeout: int) -> requests.Response:
        try:
            if (get_proxy() or "").strip():
                return _curl_post(url, json=payload, headers=headers, timeout=timeout)  # type: ignore[return-value]
            return requests.post(
                url,
                json=payload,
                headers=headers,
                timeout=timeout,
            )
        except requests.RequestException as exc:
            raise QzAPIError(f"请求失败: {exc}") from exc

    def _cookie_request(
        self,
        endpoint: str,
        payload: Dict[str, Any],
        *,
        cookie: str,
        referer: str,
        timeout: int = 60,
        retry_on_auth_error: bool = True,
    ) -> Dict[str, Any]:
        """发送使用浏览器 cookie 的内部 API 请求。"""
        url = f"{self.base_url}{endpoint}"
        response = self._post_json(url, payload, self._browser_headers(cookie, referer), timeout)
        if response.status_code == 401 and retry_on_auth_error:
            refreshed_cookie = self.ensure_cookie(force_refresh=True).get("cookie", "")
            if refreshed_cookie:
                return self._cookie_request(
                    endpoint,
                    payload,
                    cookie=refreshed_cookie,
                    referer=referer,
                    timeout=timeout,
                    retry_on_auth_error=False,
                )
        self._raise_for_bad_status(response, "Cookie 已过期或无效，请重新获取")
        result = self._parse_json_response(response, "响应不是有效的 JSON，请检查 cookie 是否正确")
        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}",
                result.get("code")
            )
        return result.get("data", {})
    
    def _get_token(self, force_refresh: bool = False) -> str:
        """获取 Access Token（带缓存）"""
        if not force_refresh and self._token:
            return self._token
        
        # 尝试从缓存获取
        if not force_refresh:
            cache = get_token_cache()
            if cache:
                self._token = cache["token"]
                return self._token
        
        # 请求新 token
        username, password = self._resolve_credentials()
        if not username or not password:
            raise QzAPIError("未配置认证信息，请运行 qzcli init 或设置环境变量 QZCLI_USERNAME/QZCLI_PASSWORD")
        
        url = f"{self.base_url}/auth/token"
        response = self._post_json(
            url,
            {"username": username, "password": password},
            {"Content-Type": "application/json"},
            30,
        )
        self._raise_for_bad_status(response, "获取 Token 失败: 认证失效")
        data = self._parse_json_response(response, "获取 Token 失败: 响应不是有效的 JSON")
        if data.get("code") != 0:
            raise QzAPIError(f"获取 Token 失败: {data.get('message', '未知错误')}", data.get("code"))
        
        # Token 可能在顶层或 data 字段中
        token_data = data.get("data", data)
        self._token = token_data.get("access_token")
        if not self._token:
            raise QzAPIError("响应中未包含 access_token")
        
        expires_in_str = token_data.get("expires_in", "604800")
        expires_in = int(expires_in_str) if isinstance(expires_in_str, str) else expires_in_str
        save_token_cache(self._token, expires_in)
        
        return self._token
    
    def _request(self, endpoint: str, data: Dict[str, Any], retry_on_auth_error: bool = True) -> Dict[str, Any]:
        """发送 API 请求"""
        token = self._get_token()
        url = f"{self.base_url}{endpoint}"

        response = self._post_json(
            url,
            data,
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            60,
        )
        self._raise_for_bad_status(response, "Token 已过期或无效，请重新认证")
        result = self._parse_json_response(response, "响应不是有效的 JSON")
        
        # Token 过期时重试
        if result.get("code") == -1 and retry_on_auth_error:
            clear_token_cache()
            self._token = None
            return self._request(endpoint, data, retry_on_auth_error=False)
        
        if result.get("code") != 0:
            raise QzAPIError(
                f"API 请求失败: {result.get('message', '未知错误')}",
                result.get("code")
            )
        
        return result

    def _request_v2(
        self,
        service: str,
        action: str,
        body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """POST 到 /api/v2/{service}?Action={action}，使用 CAS cookie 认证。"""
        cookie_data = get_cookie()
        cookie = cookie_data.get("cookie") if cookie_data else None
        if not cookie:
            raise QzAPIError(
                "v2 API 需要 cookie 认证，但本地没有有效 cookie。"
                "请先运行 `qzcli login -u <学工号> -p <密码>` 获取 CAS 会话。"
            )

        url = f"{self.base_url}/api/v2/{service}"
        headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "application/json",
            "cookie": cookie,
            "origin": self.base_url,
            "referer": f"{self.base_url}/jobs",
            "user-agent": V2_BROWSER_UA,
            "x-inspire-client-source": V2_CLIENT_SOURCE,
        }
        response = _curl_post(
            url,
            params={"Action": action},
            json=body,
            headers=headers,
            timeout=60,
        )

        if response.status_code == 401:
            raise QzAPIError(
                "Cookie 已过期或无效，请运行 `qzcli login` 重新获取",
                401,
            )

        ctype = response.headers.get("Content-Type", "")
        if "application/json" not in ctype:
            snippet = response.text[:200].replace("\n", " ")
            raise QzAPIError(
                f"v2 API 返回非 JSON（{response.status_code}, content-type={ctype}）。"
                "通常表示认证失败、APISIX 网关拒绝、或当前 cookie 无该工作空间权限。"
                f"试试 `qzcli login`。响应片段: {snippet}",
                response.status_code,
            )

        try:
            result = response.json()
        except ValueError as e:
            raise QzAPIError(f"v2 API 响应不是合法 JSON: {e}", response.status_code)

        if response.status_code >= 400:
            raise QzAPIError(
                f"v2 API 请求失败 ({response.status_code}): {result}",
                response.status_code,
            )
        return result
    
    @staticmethod
    def is_hpc_job_id(job_id: str) -> bool:
        """Return whether a job id belongs to the HPC job API family."""
        return str(job_id).startswith("hpc-job-")

    def get_train_job_detail(self, job_id: str) -> Dict[str, Any]:
        """查询训练任务详情"""
        result = self._request("/openapi/v1/train_job/detail", {"job_id": job_id})
        return result.get("data", {})

    def get_hpc_job_detail(self, job_id: str) -> Dict[str, Any]:
        """查询 HPC 任务详情"""
        result = self._request("/openapi/v1/hpc_jobs/detail", {"job_id": job_id})
        return result.get("data", {})

    def get_job_detail(self, job_id: str) -> Dict[str, Any]:
        """查询任务详情，按 job id 自动选择训练或 HPC 接口。"""
        if self.is_hpc_job_id(job_id):
            return self.get_hpc_job_detail(job_id)
        return self.get_train_job_detail(job_id)

    def _resolve_pod_names(self, job_id: str, n_instances: Optional[int] = None) -> List[str]:
        """推断 job 的 worker pod 名。"""
        if n_instances is None:
            try:
                detail = self.get_job_detail(job_id)
                framework_config = detail.get("framework_config")
                if (
                    isinstance(framework_config, list)
                    and framework_config
                    and isinstance(framework_config[0], dict)
                ):
                    n_instances = framework_config[0].get("instance_count")
                if not n_instances:
                    n_instances = (
                        detail.get("instance_count")
                        or detail.get("instances")
                        or detail.get("replica_count")
                    )
            except Exception:
                n_instances = None
        if not n_instances or n_instances < 1:
            n_instances = 1
        return [f"{job_id}-worker-{i}" for i in range(n_instances)]

    def get_job_logs(
        self,
        job_id: str,
        page_size: int = 200,
        pod_names: Optional[List[str]] = None,
        start_timestamp_ms: Optional[str] = None,
        end_timestamp_ms: Optional[str] = None,
        sort: str = "ascend",
    ) -> Dict[str, Any]:
        """拉取 train job 的容器日志（v2 GetJobLog）。"""
        if pod_names is None:
            pod_names = self._resolve_pod_names(job_id)

        body: Dict[str, Any] = {
            "page_size": page_size,
            "filter": {"podNames": pod_names},
            "sorter": [
                {"field": "time", "sort": sort},
                {"field": "log-id.keyword", "sort": sort},
            ],
        }
        if start_timestamp_ms is not None:
            body["filter"]["start_timestamp_ms"] = str(start_timestamp_ms)
        if end_timestamp_ms is not None:
            body["filter"]["end_timestamp_ms"] = str(end_timestamp_ms)

        result = self._request_v2("train", "GetJobLog", body)
        if isinstance(result.get("Result"), dict):
            result = result["Result"]
        return result
    
    def get_jobs_detail(self, job_ids: List[str], max_workers: int = 5) -> Dict[str, Dict[str, Any]]:
        """批量查询任务详情（并发）"""
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_job = {
                executor.submit(self.get_job_detail, job_id): job_id
                for job_id in job_ids
            }
            
            for future in as_completed(future_to_job):
                job_id = future_to_job[future]
                try:
                    results[job_id] = future.result()
                except Exception as e:
                    results[job_id] = {"error": str(e)}
        
        return results
    
    def stop_train_job(self, job_id: str) -> Dict[str, Any]:
        """停止训练任务"""
        result = self._request("/openapi/v1/train_job/stop", {"job_id": job_id})
        return result.get("data", result)

    def stop_hpc_job(self, job_id: str) -> Dict[str, Any]:
        """停止 HPC 任务"""
        try:
            result = self._request("/openapi/v1/hpc_jobs/stop", {"job_id": job_id})
            return result.get("data", result)
        except QzAPIError as exc:
            try:
                detail = self.get_hpc_job_detail(job_id)
            except QzAPIError:
                raise exc
            status = str(detail.get("status", "")).upper()
            if status in {"STOPPED", "FAILED", "SUCCEEDED", "SUCCESS", "COMPLETED", "FINISHED"}:
                return {
                    "job_id": job_id,
                    "already_terminal": True,
                    "status": detail.get("status", ""),
                }
            raise exc

    def stop_job(self, job_id: str) -> bool:
        """停止任务，按 job id 自动选择训练或 HPC 接口。"""
        try:
            if self.is_hpc_job_id(job_id):
                self.stop_hpc_job(job_id)
            else:
                self.stop_train_job(job_id)
            return True
        except QzAPIError:
            return False

    def stop_job_result(self, job_id: str) -> Dict[str, Any]:
        """停止任务并返回详细结果"""
        try:
            if self.is_hpc_job_id(job_id):
                data = self.stop_hpc_job(job_id)
            else:
                data = self.stop_train_job(job_id)
            return {"job_id": job_id, "stopped": True, "error": "", "code": 0, "data": data}
        except QzAPIError as exc:
            return {
                "job_id": job_id,
                "stopped": False,
                "error": str(exc) or "stop_failed",
                "code": exc.code,
            }
    
    def create_job(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """创建任务"""
        result = self._request("/openapi/v1/train_job/create", config)
        return result.get("data", result)

    def create_job_with_cookie(self, cookie: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """使用 cookie 创建分布式训练任务（内部 API）。"""
        workspace_id = config.get("workspace_id", "")
        data = self._cookie_request(
            "/api/v1/train_job/create",
            config,
            cookie=cookie,
            referer=f"https://qz.sii.edu.cn/jobs/distributedTraining?spaceId={workspace_id}",
        )
        return data

    def create_hpc_job(self, *args, **kwargs) -> Dict[str, Any]:
        """创建 HPC 任务。

        兼容两种调用方式：
        - ``create_hpc_job(config_dict)``：本地 fork 的 OpenAPI payload。
        - ``create_hpc_job(cookie=..., job_name=..., ...)``：上游内部 cookie API。
        """
        if args and isinstance(args[0], dict) and not kwargs:
            result = self._request("/openapi/v1/hpc_jobs/create", args[0])
            return result.get("data", result)
        if "config" in kwargs and isinstance(kwargs["config"], dict):
            result = self._request("/openapi/v1/hpc_jobs/create", kwargs["config"])
            return result.get("data", result)

        cookie = kwargs.get("cookie") or (args[0] if args else "")
        if not cookie:
            raise QzAPIError("创建 HPC 任务需要 cookie")

        job_name = kwargs["job_name"]
        workspace_id = kwargs["workspace_id"]
        project_id = kwargs["project_id"]
        logic_compute_group_id = kwargs["logic_compute_group_id"]
        entrypoint = kwargs["entrypoint"]
        image = kwargs["image"]
        predef_quota_id = kwargs["predef_quota_id"]
        cpu = int(kwargs["cpu"])
        mem_gi = int(kwargs["mem_gi"])
        instances = int(kwargs.get("instances", 1))
        cpus_per_task = int(kwargs.get("cpus_per_task", 1))
        memory_per_cpu = kwargs.get("memory_per_cpu", "5G")
        image_type = kwargs.get("image_type", "SOURCE_PRIVATE")
        max_running_time_days = int(kwargs.get("max_running_time_days", 0))
        max_running_time_hours = int(kwargs.get("max_running_time_hours", 0))
        max_running_time_minutes = int(kwargs.get("max_running_time_minutes", 0))

        payload = {
            "job_name": job_name,
            "workspace_id": workspace_id,
            "project_id": project_id,
            "logic_compute_group_id": logic_compute_group_id,
            "enable_notification": False,
            "dataset_info": [],
            "sbatch_script": {
                "number_of_tasks": instances,
                "cpus_per_task": cpus_per_task,
                "memory_per_cpu": memory_per_cpu,
                "enable_hyper_threading": False,
                "max_running_time_days": max_running_time_days,
                "max_running_time_hours": max_running_time_hours,
                "max_running_time_minutes": max_running_time_minutes,
                "entrypoint": entrypoint,
            },
            "slurm_cluster_spec": {
                "predef_quota_id": predef_quota_id,
                "cpu": cpu,
                "mem_gi": mem_gi,
                "image": image,
                "image_type": image_type,
                "instance_count": instances,
                "spec_price": {
                    "cpu_type": "",
                    "cpu_count": cpu,
                    "gpu_type": "",
                    "gpu_count": 0,
                    "memory_size_gib": mem_gi,
                    "logic_compute_group_id": logic_compute_group_id,
                    "quota_id": predef_quota_id,
                },
            },
        }
        return self._cookie_request(
            "/api/v1/hpc_jobs",
            payload,
            cookie=cookie,
            referer=f"https://qz.sii.edu.cn/jobs/hpc?spaceId={workspace_id}",
        )
    
    def test_connection(self) -> bool:
        """测试连接"""
        try:
            self._get_token(force_refresh=True)
            return True
        except Exception:
            return False
    
    def list_workspace_tasks(
        self, 
        workspace_id: str, 
        cookie: str,
        page_num: int = 1,
        page_size: int = 100,
        project_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        获取工作空间任务概览；旧分页参数仍兼容任务维度查询。
        
        Args:
            workspace_id: 工作空间 ID
            cookie: 浏览器 cookie 字符串
            page_num: 页码
            page_size: 每页数量（默认 100）
            project_filter: 项目名称过滤（包含匹配）
            
        Returns:
            API 响应数据，包含 task_dimensions 列表
        """
        if project_filter or page_num != 1 or page_size != 100:
            payload = {
                "page_num": page_num,
                "page_size": page_size,
                "filter": {"workspace_id": workspace_id}
            }
            data = self._cookie_request(
                "/api/v1/workspace/list_task_dimension",
                payload,
                cookie=cookie,
                referer=f"https://qz.sii.edu.cn/jobs/spacesOverview?spaceId={workspace_id}",
            )

            if project_filter:
                tasks = data.get("task_dimensions", [])
                data["task_dimensions"] = [
                    t
                    for t in tasks
                    if project_filter in t.get("project", {}).get("name", "")
                ]
            return data

        import time as _time

        end_ts = int(_time.time())
        start_ts = end_ts - 24 * 3600
        payload = {
            "filter": {"workspace_id": workspace_id},
            "time_range": {
                "start_timestamp": str(start_ts),
                "end_timestamp": str(end_ts),
            },
        }
        return self._cookie_request(
            "/api/v1/cluster_metric/overview_task_metric",
            payload,
            cookie=cookie,
            referer=f"https://qz.sii.edu.cn/jobs/spacesOverview?spaceId={workspace_id}",
        )
    
    def list_jobs_with_cookie(
        self,
        workspace_id: str,
        cookie: str,
        page_num: int = 1,
        page_size: int = 100,
        created_by: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        使用 cookie 获取任务列表（内部 API）
        
        Args:
            workspace_id: 工作空间 ID
            cookie: 浏览器 cookie 字符串
            page_num: 页码
            page_size: 每页数量
            created_by: 创建者用户 ID（可选，不传则获取所有）
            status: 原始状态过滤，例如 job_queuing、job_running
            
        Returns:
            包含 jobs 列表和 total 的字典
        """
        payload = {
            "page_num": page_num,
            "page_size": page_size,
            "workspace_id": workspace_id,
        }
        
        if created_by:
            payload["created_by"] = created_by
        if status:
            payload["status"] = status
        return self._cookie_request(
            "/api/v1/train_job/list",
            payload,
            cookie=cookie,
            referer=f"https://qz.sii.edu.cn/jobs/distributedTraining?spaceId={workspace_id}",
        )

    def list_notebooks_with_cookie(
        self,
        workspace_id: str,
        cookie: str,
        page: int = 1,
        page_size: int = 50,
        user_ids: Optional[List[str]] = None,
        status: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """使用 cookie 获取交互式建模实例列表（开发机）。"""
        payload = {
            "workspace_id": workspace_id,
            "page": page,
            "page_size": page_size,
            "filter_by": {
                "keyword": "",
                "user_id": user_ids or [],
                "logic_compute_group_id": [],
                "status": status or [],
                "mirror_url": [],
            },
            "order_by": [{"field": "created_at", "order": "desc"}],
        }
        return self._cookie_request(
            "/api/v1/notebook/list",
            payload,
            cookie=cookie,
            referer=f"https://qz.sii.edu.cn/jobs/interactiveModeling?spaceId={workspace_id}",
        )

    def list_hpc_jobs_with_cookie(
        self,
        workspace_id: str,
        cookie: str,
        page_num: int = 1,
        page_size: int = 100,
        created_by: Optional[str] = None,
        status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        使用 cookie 获取 HPC 任务列表（内部 API）。

        这个接口对应平台 HPC 任务页，适合查看当前用户的 HPC 任务历史，
        包括 QUEUEING/RUNNING 等不一定出现在 task dimension 资源视图里的状态。
        created_by 与 status 是平台支持的顶层过滤字段。

        Args:
            workspace_id: 工作空间 ID
            cookie: 浏览器 cookie 字符串
            page_num: 页码
            page_size: 每页数量
            created_by: 创建者用户 ID（可选）
            status: 原始状态过滤，例如 QUEUEING、RUNNING、SUCCEEDED

        Returns:
            包含 jobs 列表和 total 的字典
        """
        payload = {
            "page_num": page_num,
            "page_size": page_size,
            "workspace_id": workspace_id,
        }

        if created_by:
            payload["created_by"] = created_by
        if status:
            payload["status"] = status

        return self._cookie_request(
            "/api/v1/hpc_jobs/list",
            payload,
            cookie=cookie,
            referer=f"https://qz.sii.edu.cn/jobs/hpc?spaceId={workspace_id}",
        )

    def list_hpc_jobs(
        self,
        workspace_id: str,
        cookie: str,
        status: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        """兼容上游命名的 HPC 任务列表接口。"""
        return self.list_hpc_jobs_with_cookie(
            workspace_id,
            cookie,
            page_num=page_num,
            page_size=page_size,
            status=status,
        )
    
    def extract_resources_from_jobs(self, jobs: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        从任务列表中提取资源配置信息
        
        Args:
            jobs: 任务列表
            
        Returns:
            包含 workspaces, projects, compute_groups, specs 的字典
        """
        workspaces = {}
        projects = {}
        compute_groups = {}
        specs = {}
        
        for job in jobs:
            # 提取 workspace
            ws_id = job.get("workspace_id", "")
            if ws_id and ws_id not in workspaces:
                workspaces[ws_id] = {"id": ws_id}
            
            # 提取 project
            proj_id = job.get("project_id", "")
            proj_name = job.get("project_name", "")
            if proj_id and proj_id not in projects:
                projects[proj_id] = {
                    "id": proj_id,
                    "name": proj_name,
                    "en_name": job.get("project_en_name", ""),
                    "workspace_id": ws_id,
                }
            
            # 提取 compute group
            lcg_id = job.get("logic_compute_group_id", "")
            lcg_name = job.get("logic_compute_group_name", "")
            if lcg_id and lcg_id not in compute_groups:
                # 从 framework_config 中提取 GPU 信息
                gpu_info = {}
                fc = job.get("framework_config", [])
                if fc:
                    spec_info = fc[0].get("instance_spec_price_info", {})
                    gpu_info = spec_info.get("gpu_info", {})
                
                compute_groups[lcg_id] = {
                    "id": lcg_id,
                    "name": lcg_name,
                    "workspace_id": ws_id,
                    "gpu_type": gpu_info.get("gpu_product_simple", ""),
                    "gpu_type_display": gpu_info.get("gpu_type_display", ""),
                }
            
            # 提取 spec (quota_id)
            fc = job.get("framework_config", [])
            if fc:
                spec_info = fc[0].get("instance_spec_price_info", {})
                spec_id = spec_info.get("quota_id", "")
                existing_group_ids = list(
                    (specs.get(spec_id) or {}).get("logic_compute_group_ids", [])
                )
                if lcg_id and lcg_id not in existing_group_ids:
                    existing_group_ids.append(lcg_id)
                if spec_id and spec_id not in specs:
                    specs[spec_id] = {
                        "id": spec_id,
                        "logic_compute_group_id": lcg_id,
                        "logic_compute_group_ids": existing_group_ids,
                        "gpu_count": spec_info.get("gpu_count", 0),
                        "cpu_count": spec_info.get("cpu_count", 0),
                        "memory_gb": spec_info.get("memory_size_gib", 0),
                        "gpu_type": spec_info.get("gpu_info", {}).get("gpu_product_simple", ""),
                        "gpu_type_display": spec_info.get("gpu_info", {}).get("gpu_type_display", ""),
                    }
                elif spec_id and existing_group_ids:
                    specs[spec_id]["logic_compute_group_ids"] = existing_group_ids
        
        return {
            "workspaces": list(workspaces.values()),
            "projects": list(projects.values()),
            "compute_groups": list(compute_groups.values()),
            "specs": list(specs.values()),
        }
    
    def list_specs(self, compute_group_id: str) -> List[Dict[str, Any]]:
        """
        获取计算组可用的规格列表（使用 OpenAPI）
        
        Args:
            compute_group_id: 计算组 ID
            
        Returns:
            规格列表
        """
        result = self._request("/openapi/v1/specs/list", {"logic_compute_group_id": compute_group_id})
        return result.get("data", {}).get("specs", [])

    def list_resource_spec_prices(
        self,
        workspace_id: str,
        logic_compute_group_id: str,
        cookie: str,
        schedule_config_type: str = "SCHEDULE_CONFIG_TYPE_HPC",
    ) -> List[Dict[str, Any]]:
        """
        获取指定逻辑计算组下可用的资源规格价格信息。

        这是创建页使用的内部 cookie API；返回项中的 quota_id 可作为创建任务
        payload 里的 spec_id 使用。
        """
        payload = {
            "workspace_id": workspace_id,
            "logic_compute_group_id": logic_compute_group_id,
            "schedule_config_type": schedule_config_type,
        }
        data = self._cookie_request(
            "/api/v1/resource_prices/logic_compute_groups/",
            payload,
            cookie=cookie,
            referer=f"https://qz.sii.edu.cn/jobs/create?spaceId={workspace_id}",
        )
        return data.get("lcg_resource_spec_prices", [])
    
    def list_node_dimension(
        self,
        workspace_id: str,
        cookie: str,
        logic_compute_group_id: Optional[str] = None,
        compute_group_id: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 100,
    ) -> Dict[str, Any]:
        """
        获取节点维度的资源使用情况（使用浏览器 cookie 认证）
        
        Args:
            workspace_id: 工作空间 ID
            cookie: 浏览器 cookie 字符串
            logic_compute_group_id: 计算组 ID（可选）
            page_num: 页码
            page_size: 每页数量
            
        Returns:
            包含 node_dimensions 列表的字典
        """
        filter_params = {"workspace_id": workspace_id}
        if logic_compute_group_id:
            filter_params["logic_compute_group_id"] = logic_compute_group_id
        if compute_group_id:
            filter_params["compute_group_id"] = compute_group_id
        
        payload = {
            "page_num": page_num,
            "page_size": page_size,
            "filter": filter_params,
        }
        return self._cookie_request(
            "/api/v1/cluster_metric/list_node_dimension",
            payload,
            cookie=cookie,
            referer=f"https://qz.sii.edu.cn/jobs/spacesOverview?spaceId={workspace_id}",
        )

    def list_task_dimension(
        self,
        workspace_id: str,
        cookie: str,
        project_id: Optional[str] = None,
        page_num: int = 1,
        page_size: int = 200,
    ) -> Dict[str, Any]:
        """
        获取任务维度的资源使用情况（使用浏览器 cookie 认证）
        
        Args:
            workspace_id: 工作空间 ID
            cookie: 浏览器 cookie 字符串
            project_id: 项目 ID（可选）
            page_num: 页码
            page_size: 每页数量
            
        Returns:
            包含 task_dimensions 列表的字典
        """
        filter_params = {"workspace_id": workspace_id}
        if project_id:
            filter_params["project_id"] = project_id
        
        payload = {
            "page_num": page_num,
            "page_size": page_size,
            "filter": filter_params,
        }
        return self._cookie_request(
            "/api/v1/cluster_metric/list_task_dimension",
            payload,
            cookie=cookie,
            referer=f"https://qz.sii.edu.cn/jobs/spacesOverview?spaceId={workspace_id}",
        )

    def get_cluster_basic_info(self, workspace_id: str, cookie: str) -> Dict[str, Any]:
        """
        获取工作空间的集群和计算组信息
        
        Args:
            workspace_id: 工作空间 ID
            cookie: 浏览器 cookie 字符串
            
        Returns:
            包含 clusters, compute_groups, resource_types 的字典
        """
        payload = {
            "workspace_id": workspace_id
        }
        return self._cookie_request(
            "/api/v1/cluster_metric/cluster_basic_info",
            payload,
            cookie=cookie,
            referer=f"https://qz.sii.edu.cn/jobs/spacesOverview?spaceId={workspace_id}",
        )

    def list_workspaces(self, cookie: str) -> List[Dict[str, Any]]:
        """
        获取用户可访问的工作空间列表
        
        通过 /api/v1/project/list 获取项目列表，从中提取工作空间信息。
        每个项目的 space_list 字段包含该项目关联的工作空间。
        
        Args:
            cookie: 浏览器 cookie 字符串
            
        Returns:
            工作空间列表 [{"id": "ws-xxx", "name": "工作空间名称"}, ...]
        """
        workspaces = {}
        page = 1
        page_size = 100

        while True:
            payload = {
                "page": page,
                "page_size": page_size,
                "filter": {}
            }
            data = self._cookie_request(
                "/api/v1/project/list",
                payload,
                cookie=cookie,
                referer="https://qz.sii.edu.cn/operations/projects",
            )
            items = data.get("items", [])

            for proj in items:
                space_list = proj.get("space_list", [])
                for space in space_list:
                    ws_id = space.get("id", "")
                    ws_name = space.get("name", "")
                    if ws_id and ws_id not in workspaces:
                        workspaces[ws_id] = {
                            "id": ws_id,
                            "name": ws_name,
                        }

            total = data.get("total")
            try:
                total_count = int(total) if total is not None else None
            except (TypeError, ValueError):
                total_count = None

            if not items:
                break
            if total_count is not None and page * page_size >= total_count:
                break
            if len(items) < page_size:
                break
            page += 1
        
        return list(workspaces.values())

    @staticmethod
    def _has_session_cookie(cookies: Dict[str, str]) -> bool:
        """Check if any session-like cookie exists (handles name changes like session -> inspire-session)."""
        return any("session" in name.lower() for name in cookies)

    def login_with_cas(self, username: str, password: str) -> str:
        """
        通过 CAS 统一认证登录，获取 session cookie
        
        登录流程：
        1. 访问 qz.sii.edu.cn -> 重定向到 Keycloak
        2. Keycloak 重定向到 CAS 登录页
        3. 在 CAS 提交用户名密码
        4. CAS 验证后重定向回 Keycloak
        5. Keycloak 重定向回 qz.sii.edu.cn，设置 session cookie
        
        Args:
            username: CAS 用户名（学工号）
            password: CAS 密码
            
        Returns:
            session cookie 字符串
        """
        import re
        from urllib.parse import urljoin, urlparse, parse_qs
        
        session = requests.Session()

        proxy = get_proxy()
        if proxy:
            # WSL/VPN setups often export HTTP(S)_PROXY; trust_env=False prevents
            # those env vars from overriding an explicit qzcli SOCKS proxy.
            session.trust_env = False
            proxy_url = proxy.replace("socks5h://", "socks5://")
            session.proxies = {"http": proxy_url, "https": proxy_url}
        
        # 设置浏览器 User-Agent
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        }
        session.headers.update(headers)
        
        # Step 1: 访问启智平台，触发 OAuth 流程
        try:
            resp = session.get(self.base_url, timeout=30, allow_redirects=True)
        except requests.RequestException as e:
            raise QzAPIError(f"无法连接到启智平台: {e}")
        
        current_url = resp.url
        current_host = urlparse(current_url).netloc
        
        # 如果已经在启智平台且有 session cookie，说明已登录
        if current_host == "qz.sii.edu.cn":
            qz_cookies = {}
            for cookie in session.cookies:
                if "qz.sii.edu.cn" in cookie.domain:
                    qz_cookies[cookie.name] = cookie.value
            if self._has_session_cookie(qz_cookies):
                cookie_str = "; ".join([f"{k}={v}" for k, v in qz_cookies.items()])
                return cookie_str
        
        # Step 2: 如果在 Keycloak，需要继续到 CAS
        if "keycloak" in current_url:
            # Keycloak 页面使用 JavaScript 渲染，CAS URL 在 kcContext 对象中
            # 查找 providers 中的 CAS loginUrl
            cas_url_match = re.search(r'"loginUrl":\s*"([^"]*broker/cas/login[^"]*)"', resp.text)
            if cas_url_match:
                cas_broker_url = cas_url_match.group(1)
                # 处理转义的斜杠
                cas_broker_url = cas_broker_url.replace("\\/", "/")
                if not cas_broker_url.startswith("http"):
                    # 相对 URL，需要拼接
                    parsed = urlparse(current_url)
                    cas_broker_url = f"{parsed.scheme}://{parsed.netloc}{cas_broker_url}"
                
                try:
                    resp = session.get(cas_broker_url, timeout=30, allow_redirects=True)
                    current_url = resp.url
                except requests.RequestException as e:
                    raise QzAPIError(f"跳转 CAS 失败: {e}")
            else:
                raise QzAPIError("Keycloak 页面中未找到 CAS 登录链接")
        
        # Step 3: 检查是否在 CAS 登录页
        if "cas.sii.edu.cn" not in current_url:
            raise QzAPIError(f"未能到达 CAS 登录页面，当前 URL: {current_url}")
        
        cas_login_url = current_url
        login_page_html = resp.text

        encrypted_password = encrypt_password(password)

        lt_match = re.search(r'name="lt"\s+value="([^"]+)"', login_page_html)
        execution_match = re.search(r'name="execution"\s+value="([^"]+)"', login_page_html)

        login_data = {
            "username": username,
            "password": encrypted_password,
            "_eventId": "submit",
            "submit": "登 录",
            "loginType": "1",
            "encrypted": "true",
        }

        if lt_match:
            login_data["lt"] = lt_match.group(1)
        if execution_match:
            login_data["execution"] = execution_match.group(1)
        
        # Step 5: 提交登录表单
        login_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": "https://cas.sii.edu.cn",
            "Referer": cas_login_url,
        }
        
        try:
            resp = session.post(
                cas_login_url,
                data=login_data,
                headers=login_headers,
                timeout=30,
                allow_redirects=True,
            )
        except requests.RequestException as e:
            raise QzAPIError(f"登录请求失败: {e}")
        
        current_url = resp.url
        
        # Step 6: 检查登录结果
        if "cas.sii.edu.cn" in current_url and "login" in current_url:
            # 仍然在登录页，可能是密码错误
            if "用户名或密码错误" in resp.text or "账号或密码错误" in resp.text:
                raise QzAPIError("用户名或密码错误")
            if "验证码" in resp.text:
                raise QzAPIError("需要输入验证码，请在浏览器中登录后手动获取 cookie")
            raise QzAPIError("登录失败，请检查用户名和密码")
        
        # Step 7: 确保完成所有重定向回到启智平台
        current_host = urlparse(current_url).netloc
        if current_host != "qz.sii.edu.cn":
            # 可能还需要额外访问启智平台来完成 session 设置
            try:
                resp = session.get(self.base_url, timeout=30, allow_redirects=True)
            except requests.RequestException as e:
                raise QzAPIError(f"获取 session 失败: {e}")
        
        # 收集所有 qz.sii.edu.cn 域的 cookies
        all_cookies = {}
        for cookie in session.cookies:
            # 检查是否是 qz.sii.edu.cn 的 cookie
            if "qz.sii.edu.cn" in cookie.domain:
                all_cookies[cookie.name] = cookie.value
        
        if not all_cookies or not self._has_session_cookie(all_cookies):
            try:
                resp = session.get(self.base_url, timeout=30, allow_redirects=True)
                for cookie in session.cookies:
                    if "qz.sii.edu.cn" in cookie.domain:
                        all_cookies[cookie.name] = cookie.value
            except:
                pass
        
        if not all_cookies or not self._has_session_cookie(all_cookies):
            raise QzAPIError("登录成功但未获取到 session cookie")
        
        # 构建 cookie 字符串（确保 session 和 session_2 都包含）
        cookie_str = "; ".join([f"{k}={v}" for k, v in all_cookies.items()])
        
        return cookie_str


# 全局 API 实例（延迟初始化）
_api_instance: Optional[QzAPI] = None


def get_api() -> QzAPI:
    """获取全局 API 实例"""
    global _api_instance
    if _api_instance is None:
        _api_instance = QzAPI()
    return _api_instance
