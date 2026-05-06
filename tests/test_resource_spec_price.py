import unittest

from qzcli.api import build_resource_spec_price


class BuildResourceSpecPriceTests(unittest.TestCase):
    def test_full_spec_produces_all_six_payload_fields(self):
        spec_obj = {
            "id": "quota-h200-8",
            "cpu_count": 180,
            "gpu_count": 8,
            "memory_gb": 1800,
            "gpu_type": "NVIDIA_H200_SXM_141G",
            "gpu_type_display": "H200",
            "logic_compute_group_id": "lcg-other",
        }

        result = build_resource_spec_price(spec_obj, "lcg-abc")

        self.assertEqual(
            {
                "cpu_type": "",
                "cpu_count": 180,
                "gpu_type": "NVIDIA_H200_SXM_141G",
                "gpu_count": 8,
                "memory_size_gib": 1800,
                "logic_compute_group_id": "lcg-abc",
                "quota_id": "quota-h200-8",
            },
            result,
        )

    def test_compute_group_id_overrides_spec_objs_own_lcg(self):
        spec_obj = {
            "id": "q",
            "cpu_count": 1,
            "gpu_count": 1,
            "memory_gb": 8,
            "gpu_type": "X",
            "logic_compute_group_id": "lcg-stale-from-cache",
        }

        result = build_resource_spec_price(spec_obj, "lcg-current")

        self.assertEqual("lcg-current", result["logic_compute_group_id"])

    def test_memory_gb_cache_field_translates_to_memory_size_gib_payload_field(self):
        spec_obj = {"id": "q", "memory_gb": 256}

        result = build_resource_spec_price(spec_obj, "lcg")

        self.assertNotIn("memory_gb", result)
        self.assertEqual(256, result["memory_size_gib"])

    def test_missing_fields_default_to_zero_or_empty_string(self):
        result = build_resource_spec_price({}, "lcg")

        self.assertEqual(
            {
                "cpu_type": "",
                "cpu_count": 0,
                "gpu_type": "",
                "gpu_count": 0,
                "memory_size_gib": 0,
                "logic_compute_group_id": "lcg",
                "quota_id": "",
            },
            result,
        )

    def test_string_numeric_fields_are_coerced_to_int(self):
        spec_obj = {
            "id": "q",
            "cpu_count": "32",
            "gpu_count": "1",
            "memory_gb": "64",
            "gpu_type": "A100",
        }

        result = build_resource_spec_price(spec_obj, "lcg")

        self.assertEqual(32, result["cpu_count"])
        self.assertEqual(1, result["gpu_count"])
        self.assertEqual(64, result["memory_size_gib"])
        self.assertIsInstance(result["cpu_count"], int)


if __name__ == "__main__":
    unittest.main()
