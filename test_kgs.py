import os
import unittest
import yaml
from kgs import K8SManifest


class TestManifests(unittest.TestCase):
    def test_manifest_load(self):
        testdata_map = {}
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_kgs.yaml")
        with open(path, "r") as f:
            testdata_map = yaml.safe_load(f)

        for typ, testdata_list in testdata_map.items():
            cls = None
            if typ == "k8s":
                cls = K8SManifest

            data = {}
            for data in testdata_list:
                actual = cls.parse_dict(data["in"])
                expect = cls.from_dict(data["expect"])
                self.assertEqual(expect, actual)
