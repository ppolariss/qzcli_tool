"""v2 Console API: create_job_v2 endpoint/headers + 响应封装 _unwrap_v2_result。"""
import unittest
from unittest import mock

from qzcli import api
from qzcli.api import QzAPIError, _unwrap_v2_result


class UnwrapV2Tests(unittest.TestCase):
    def test_result_dict_returned(self):
        self.assertEqual(
            _unwrap_v2_result({"Result": {"job_id": "j-1"}}), {"job_id": "j-1"}
        )

    def test_legacy_data_fallback(self):
        self.assertEqual(_unwrap_v2_result({"data": {"job_id": "j-2"}}), {"job_id": "j-2"})

    def test_response_metadata_error_raises(self):
        with self.assertRaises(QzAPIError) as cm:
            _unwrap_v2_result({"ResponseMetadata": {"Error": {
                "Code": "InvalidNode", "Message": "node not ready"}}})
        self.assertIn("InvalidNode", str(cm.exception))
        self.assertIn("node not ready", str(cm.exception))

    def test_legacy_code_nonzero_raises(self):
        with self.assertRaises(QzAPIError):
            _unwrap_v2_result({"code": 500, "message": "boom"})

    def test_code_zero_ok(self):
        self.assertEqual(_unwrap_v2_result({"code": 0, "Result": {"ok": 1}}), {"ok": 1})

    def test_empty_when_no_payload(self):
        self.assertEqual(_unwrap_v2_result({"Result": None}), {})
        self.assertEqual(_unwrap_v2_result("nope"), {})


class _Resp:
    def __init__(self, status, payload):
        self.status_code = status
        self._payload = payload

    def json(self):
        return self._payload


class CreateJobV2Tests(unittest.TestCase):
    def _api(self):
        a = api.QzAPI.__new__(api.QzAPI)
        a.base_url = "https://qz.sii.edu.cn"
        return a

    def test_posts_v2_console_endpoint_with_headers(self):
        seen = {}

        def fake_post(url, *, json=None, headers=None, timeout=60, **_):
            seen["url"] = url
            seen["headers"] = headers
            seen["body"] = json
            return _Resp(200, {"Result": {"job_id": "j-9"}})

        with mock.patch.object(api, "_curl_post", side_effect=fake_post):
            out = self._api().create_job_v2("ck", {"workspace_id": "ws-1", "name": "t"})

        self.assertEqual(out, {"job_id": "j-9"})
        self.assertEqual(
            seen["url"],
            "https://qz.sii.edu.cn/api/v2/train?Action=CreateJobConsole",
        )
        self.assertEqual(seen["headers"]["cookie"], "ck")
        self.assertIn("Mozilla", seen["headers"]["user-agent"])
        self.assertIn("distributedTraining", seen["headers"]["referer"])
        self.assertEqual(seen["body"]["workspace_id"], "ws-1")

    def test_401_raises(self):
        with mock.patch.object(api, "_curl_post", return_value=_Resp(401, {})):
            with self.assertRaises(QzAPIError):
                self._api().create_job_v2("ck", {})

    def test_v2_error_envelope_raises(self):
        payload = {"ResponseMetadata": {"Error": {"Code": "X", "Message": "bad"}}}
        with mock.patch.object(api, "_curl_post", return_value=_Resp(200, payload)):
            with self.assertRaises(QzAPIError):
                self._api().create_job_v2("ck", {})


if __name__ == "__main__":
    unittest.main()
