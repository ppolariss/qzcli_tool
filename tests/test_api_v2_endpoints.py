"""迁到 v2 的各端点：URL / Action / 请求体形状 + v1 回落。

对应 docs/v1_to_v2_mapping.md 的映射表。每条断言的字段名都来自真机响应，
不是照 schema 推的。
"""

import threading
import unittest
from unittest import mock

from qzcli import api
from qzcli.api import QzAPIError


class _Resp:
    def __init__(self, status, payload, content_type="application/json"):
        self.status_code = status
        self._payload = payload
        self.headers = {"Content-Type": content_type}
        self.text = str(payload)

    def json(self):
        return self._payload


def _client():
    a = api.QzAPI.__new__(api.QzAPI)
    a.base_url = "https://qz.example"
    a._username = None
    a._password = None
    a._token = None
    a._auto_relogin = False
    a._relogin_lock = threading.Lock()
    return a


class V2EndpointShapeTests(unittest.TestCase):
    """每个迁移过的端点：打到哪个 service/Action，body 长什么样。"""

    def setUp(self):
        api._V2_FALLBACK_WARNED.clear()
        self.seen = []

        def fake_post(url, *, json=None, headers=None, params=None, timeout=60, **_):
            self.seen.append({"url": url, "params": params, "body": json})
            return _Resp(200, {"Result": {"ok": True}})

        patcher = mock.patch.object(api, "_curl_post", side_effect=fake_post)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _last(self):
        return self.seen[-1]

    def test_list_jobs_hits_train_ListJobs(self):
        _client().list_jobs_with_cookie("ws-1", "ck", page_num=2, page_size=7)
        call = self._last()
        self.assertEqual(call["url"], "https://qz.example/api/v2/train")
        self.assertEqual(call["params"], {"Action": "ListJobs"})
        self.assertEqual(
            call["body"], {"workspace_id": "ws-1", "page_num": 2, "page_size": 7}
        )

    def test_list_jobs_passes_created_by(self):
        _client().list_jobs_with_cookie("ws-1", "ck", created_by="user-9")
        self.assertEqual(self._last()["body"]["created_by"], "user-9")

    def test_job_detail_hits_train_GetJob(self):
        _client().get_job_detail_with_cookie("job-1", "ck")
        call = self._last()
        self.assertEqual(call["params"], {"Action": "GetJob"})
        self.assertEqual(call["body"], {"job_id": "job-1"})

    def test_notebooks_hit_notebook_ListNotebooks(self):
        _client().list_notebooks_with_cookie("ws-1", "ck", page=3, page_size=9)
        call = self._last()
        self.assertEqual(call["url"], "https://qz.example/api/v2/notebook")
        self.assertEqual(call["params"], {"Action": "ListNotebooks"})
        # 开发机分页是 `page`，不是其他端点的 `page_num` —— 写错会静默拿第一页
        self.assertEqual(call["body"]["page"], 3)
        self.assertNotIn("page_num", call["body"])
        self.assertIn("filter_by", call["body"])
        self.assertEqual(call["body"]["order_by"][0]["order"], "desc")

    def test_node_dimension_hits_workspace_not_cluster(self):
        """cluster.ListNodeDimension 对普通账号是 AccessForbidden，
        必须打 workspace 的同名 Action。这条断言就是防回退的。"""
        _client().list_node_dimension("ws-1", "ck", logic_compute_group_id="lcg-1")
        call = self._last()
        self.assertEqual(call["url"], "https://qz.example/api/v2/workspace")
        self.assertNotIn("/cluster", call["url"])
        self.assertEqual(call["params"], {"Action": "ListNodeDimension"})
        self.assertEqual(call["body"]["filter"]["logic_compute_group_id"], "lcg-1")

    def test_task_dimension_hits_workspace_not_cluster(self):
        _client().list_task_dimension("ws-1", "ck", project_id="p-1")
        call = self._last()
        self.assertEqual(call["url"], "https://qz.example/api/v2/workspace")
        self.assertEqual(call["params"], {"Action": "ListTaskDimension"})
        self.assertEqual(call["body"]["filter"]["project_id"], "p-1")

    def test_basic_info_hits_workspace_GetBasicInfo(self):
        _client().get_cluster_basic_info("ws-1", "ck")
        call = self._last()
        self.assertEqual(call["url"], "https://qz.example/api/v2/workspace")
        self.assertEqual(call["params"], {"Action": "GetBasicInfo"})

    def test_overview_task_metric_uses_second_timestamps(self):
        """time_range 是秒级、区间 ≤1 个月。传毫秒平台会报
        `查询时间区间不能超过1个月`。"""
        _client().list_workspace_tasks("ws-1", "ck", hours=24)
        tr = self._last()["body"]["time_range"]
        span = int(tr["end_timestamp"]) - int(tr["start_timestamp"])
        self.assertEqual(span, 24 * 3600)
        # 秒级时间戳是 10 位；毫秒是 13 位
        self.assertEqual(len(tr["end_timestamp"]), 10)

    def test_hpc_list_hits_hpc_ListJobs(self):
        _client().list_hpc_jobs("ws-1", page_size=4)
        call = self._last()
        self.assertEqual(call["url"], "https://qz.example/api/v2/hpc")
        self.assertEqual(call["params"], {"Action": "ListJobs"})


class StopJobTests(unittest.TestCase):
    def setUp(self):
        api._V2_FALLBACK_WARNED.clear()

    def test_stop_hits_train_StopJob(self):
        seen = {}

        def fake_post(url, *, json=None, params=None, **_):
            seen.update(url=url, params=params, body=json)
            return _Resp(200, {"Result": {}})

        with mock.patch.object(api, "_curl_post", side_effect=fake_post):
            self.assertTrue(_client().stop_job_with_cookie("job-7", "ck"))
        self.assertEqual(seen["url"], "https://qz.example/api/v2/train")
        self.assertEqual(seen["params"], {"Action": "StopJob"})
        self.assertEqual(seen["body"], {"job_id": "job-7"})

    def test_business_error_never_double_stops(self):
        """唯一迁到 v2 的写操作。业务错误（任务已结束/无权限）必须直接抛，
        绝不能回落 v1 导致停两次。"""
        calls = []

        def fake_post(url, **_):
            calls.append(url)
            return _Resp(
                200,
                {
                    "ResponseMetadata": {
                        "Error": {"Code": "InvalidStatus", "Message": "已结束"}
                    }
                },
            )

        with mock.patch.object(api, "_curl_post", side_effect=fake_post):
            with self.assertRaises(QzAPIError):
                _client().stop_job_with_cookie("job-7", "ck")
        self.assertEqual(len(calls), 1)


class ListSpecsTests(unittest.TestCase):
    """/openapi/v1/specs/list 平台上已 404，规格只能从历史任务反推。"""

    def test_recovers_spec_ids_from_job_history(self):
        job = {
            "logic_compute_group_id": "lcg-1",
            "framework_config": [
                {
                    "gpu_count": 8,
                    "cpu": 180,
                    "mem_gi": 1800,
                    "instance_spec_price_info": {
                        "quota_id": "quota-abc",
                        "gpu_count": 8,
                        "cpu_count": 180,
                        "memory_size_gib": 1800,
                        "gpu_info": {"gpu_type": "NVIDIA_H200_SXM_141G"},
                    },
                }
            ],
        }
        client = _client()
        with mock.patch.object(
            client, "_request", side_effect=QzAPIError("404", 404)
        ), mock.patch.object(
            client, "list_jobs_with_cookie", return_value={"jobs": [job]}
        ):
            specs = client.list_specs("lcg-1", "ws-1")

        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0]["id"], "quota-abc")
        self.assertEqual(specs[0]["gpu_count"], 8)
        # gpu_type 必须是完整串，平台校验不认简称 "H200"
        self.assertEqual(specs[0]["gpu_type"], "NVIDIA_H200_SXM_141G")
        self.assertEqual(specs[0]["logic_compute_group_ids"], ["lcg-1"])

    def test_filters_out_other_compute_groups(self):
        jobs = [
            {
                "logic_compute_group_id": "lcg-other",
                "framework_config": [
                    {"instance_spec_price_info": {"quota_id": "quota-x"}}
                ],
            }
        ]
        client = _client()
        with mock.patch.object(
            client, "_request", side_effect=QzAPIError("404", 404)
        ), mock.patch.object(
            client, "list_jobs_with_cookie", return_value={"jobs": jobs}
        ):
            self.assertEqual(client.list_specs("lcg-1", "ws-1"), [])

    def test_without_workspace_id_returns_empty(self):
        """没有 workspace 就翻不了历史任务 —— 老实返回空，别硬编。"""
        client = _client()
        with mock.patch.object(client, "_request", side_effect=QzAPIError("404", 404)):
            self.assertEqual(client.list_specs("lcg-1"), [])

    def test_legacy_endpoint_wins_when_alive(self):
        """老接口还活着就用它 —— 它才是"规格清单"的权威来源，
        历史任务反推只能看到跑过的那些。"""
        client = _client()
        with mock.patch.object(
            client, "_request", return_value={"data": {"specs": [{"id": "s-1"}]}}
        ), mock.patch.object(client, "list_jobs_with_cookie") as hist:
            self.assertEqual(client.list_specs("lcg-1", "ws-1"), [{"id": "s-1"}])
        hist.assert_not_called()


class V2FallbackToV1Tests(unittest.TestCase):
    """v2 路由 404 时，公开方法要透明回落到 v1 并返回同样形状的数据。"""

    def setUp(self):
        api._V2_FALLBACK_WARNED.clear()

    def test_list_jobs_falls_back_on_404(self):
        calls = []

        def fake_post(url, *, json=None, headers=None, params=None, timeout=60, **_):
            calls.append(url)
            if "/api/v2/" in url:
                return _Resp(404, None, content_type="text/plain")
            # v1 的信封是 {code:0, data:{...}}
            return _Resp(
                200, {"code": 0, "data": {"jobs": [{"job_id": "j-1"}], "total": 1}}
            )

        with mock.patch.object(
            api, "_curl_post", side_effect=fake_post
        ), mock.patch.object(api, "print"):
            out = _client().list_jobs_with_cookie("ws-1", "ck")

        self.assertEqual(out, {"jobs": [{"job_id": "j-1"}], "total": 1})
        self.assertTrue(calls[0].endswith("/api/v2/train"))
        self.assertTrue(calls[1].endswith("/api/v1/train_job/list"))

    def test_business_error_surfaces_instead_of_falling_back(self):
        """v2 通了但报 AccessForbidden —— 应该抛出去，不能偷偷用 v1 顶上，
        否则权限问题会被永久掩盖。"""
        calls = []

        def fake_post(url, **_):
            calls.append(url)
            return _Resp(
                200,
                {
                    "ResponseMetadata": {
                        "Error": {"Code": "AccessForbidden", "Message": "denied"}
                    }
                },
            )

        with mock.patch.object(api, "_curl_post", side_effect=fake_post):
            with self.assertRaises(QzAPIError):
                _client().get_cluster_basic_info("ws-1", "ck")

        self.assertEqual(len(calls), 1)  # 没有第二次（v1）调用
        self.assertIn("/api/v2/", calls[0])


if __name__ == "__main__":
    unittest.main()
