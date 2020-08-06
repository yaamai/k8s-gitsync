import os
import unittest
from unittest import mock
import yaml
from kgs import K8SManifest, K8SOperator


class TestManifests(unittest.TestCase):
    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_kgs.yaml")
        with open(path, "r") as f:
            self.testdata = yaml.safe_load(f)

    def test_manifest_load(self):
        testdata_map = self.testdata["test_manifest_load"]
        for typ, testdata_list in testdata_map.items():
            cls = None
            if typ == "k8s":
                cls = K8SManifest

            data = {}
            for data in testdata_list:
                actual = cls.parse_dict(data["in"])
                expect = cls.from_dict(data["expect"])
                self.assertEqual(expect, actual)


class TestK8SOperator(unittest.TestCase):
    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_kgs.yaml")
        with open(path, "r") as f:
            self.testdata = yaml.safe_load(f)

    @mock.patch('kgs.cmd_exec')
    def test_operator_get_state(self, cmd_exec):
        testdata = self.testdata["test_operator_get_state"]
        cmd_exec.return_value = (b"{}", "", 0)

        for td in testdata:
            oper = K8SOperator()
            cmd_exec.reset_mock()
            manifest = K8SManifest.parse_dict(td["in"])
            ret = oper.get_state(manifest)

            if "expect" in td and td.get("expect"):
                cmd_exec.assert_called_with(td["expect"])
            if "return" in td:
                self.assertEqual(td["return"], ret)
