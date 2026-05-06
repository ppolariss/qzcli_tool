"""Outbound-payload tests for cmd_create.

Verifies the payload sent to /api/v1/train_job/create no longer contains the
deprecated framework_config[0].spec_id field, and instead nests a
resource_spec_price object alongside image/instance_count/shm_gi.
"""

import argparse
import io
import json
import unittest
from contextlib import redirect_stdout
from unittest import mock

from qzcli import cli


class _FakeAPI:
    """Minimal QzAPI stand-in. Captures whatever payload create_job receives."""

    def __init__(self):
        self.last_payload = None

    def create_job(self, payload):
        self.last_payload = payload
        return {"job_id": "job-fake-1", "workspace_id": "ws-test"}

    def create_job_with_cookie(self, cookie, payload):
        self.last_payload = payload
        return {"job_id": "job-fake-1", "workspace_id": "ws-test"}

    def list_specs(self, compute_group_id):
        return []


def _build_args(**overrides):
    args = argparse.Namespace(
        interactive=False,
        name="claude-test",
        cmd_str="echo hi",
        workspace="ws-test",
        project="project-test",
        compute_group="lcg-test",
        spec="spec-test",
        image=None,
        image_type=None,
        instances=None,
        shm=None,
        priority=None,
        framework=None,
        no_track=True,
        dry_run=False,
        output_json=True,
    )
    for k, v in overrides.items():
        setattr(args, k, v)
    return args


_FAKE_RESOURCES = {
    "ws-test": {
        "id": "ws-test",
        "name": "test-ws",
        "projects": {"project-test": {"id": "project-test", "name": "p"}},
        "compute_groups": {
            "lcg-test": {"id": "lcg-test", "name": "cg", "gpu_type": "H100"}
        },
        "specs": {
            "spec-test": {
                "id": "spec-test",
                "name": "h100-1g",
                "logic_compute_group_id": "lcg-test",
                "logic_compute_group_ids": ["lcg-test"],
                "gpu_count": 1,
                "cpu_count": 28,
                "memory_gb": 240,
                "gpu_type": "NVIDIA_H100_SXM_80G",
                "gpu_type_display": "H100",
            }
        },
    }
}


class CreatePayloadTests(unittest.TestCase):
    def _run_create(self):
        api = _FakeAPI()
        args = _build_args()

        # Patch the singletons cmd_create reaches for, plus the resource-cache
        # accessors it uses to resolve names → ids. We feed everything from
        # _FAKE_RESOURCES so no network/disk hits the real ~/.qzcli files.
        patches = [
            mock.patch("qzcli.cli.get_api", return_value=api),
            mock.patch(
                "qzcli.cli.get_store", return_value=mock.MagicMock(add_job=lambda *_: None)
            ),
            mock.patch(
                "qzcli.cli.get_workspace_resources",
                side_effect=lambda ws_id: _FAKE_RESOURCES.get(ws_id),
            ),
            mock.patch("qzcli.cli.find_workspace_by_name", return_value="ws-test"),
            # Force cookie auth path so we exercise create_job_with_cookie.
            mock.patch(
                "qzcli.cli.get_cookie", return_value={"cookie": "fake-cookie"}
            ),
            mock.patch(
                "qzcli.cli._auto_select_resource",
                return_value=("project-test", "p"),
            ),
            # cmd_create calls _validate_cached_resource_membership for project
            # and _validate_cached_spec_membership for spec when both look up
            # by name. Force True so resolution succeeds.
            mock.patch(
                "qzcli.cli._validate_cached_resource_membership",
                return_value=True,
            ),
            mock.patch(
                "qzcli.cli._validate_cached_spec_membership", return_value=True
            ),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

        rc = cli.cmd_create(args)
        return rc, api

    def test_payload_contains_resource_spec_price_and_no_spec_id(self):
        # Suppress the JSON status line cmd_create prints on success.
        with redirect_stdout(io.StringIO()):
            rc, api = self._run_create()

        self.assertEqual(0, rc)
        self.assertIsNotNone(api.last_payload, "create_job was not called")

        # Serialize and string-search to make sure spec_id is gone EVERYWHERE,
        # including any nested location.
        serialized = json.dumps(api.last_payload)
        self.assertNotIn(
            "spec_id", serialized,
            f"Legacy spec_id field leaked into payload: {serialized}",
        )

        fc = api.last_payload["framework_config"][0]
        self.assertIn("resource_spec_price", fc)
        rsp = fc["resource_spec_price"]

        # All 6 fields the platform expects, with values pulled from the spec cache.
        self.assertEqual(
            {
                "cpu_type": "",
                "cpu_count": 28,
                "gpu_type": "NVIDIA_H100_SXM_80G",
                "gpu_count": 1,
                "memory_size_gib": 240,
                "logic_compute_group_id": "lcg-test",
                "quota_id": "spec-test",
            },
            rsp,
        )

        # framework_config sibling keys still carry image/instance/shm.
        self.assertEqual(
            cli.DEFAULT_CREATE_IMAGE, fc["image"],
        )
        self.assertEqual(1, fc["instance_count"])
        self.assertEqual(cli.DEFAULT_CREATE_SHM, fc["shm_gi"])

        # Platform also requires cpu/mem_gi/gpu_count at framework_config[0]
        # alongside resource_spec_price; without these the platform returns
        # "Cpu and Mem can't be empty." (verified empirically 2026-05-06).
        self.assertEqual(28, fc["cpu"])
        self.assertEqual(240, fc["mem_gi"])
        self.assertEqual(1, fc["gpu_count"])


if __name__ == "__main__":
    unittest.main()
