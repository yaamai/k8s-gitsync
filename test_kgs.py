import os
import unittest
from functools import partial
from pathlib import Path
from unittest import mock
from unittest.mock import mock_open

import yaml

from kgs import HelmManifest
from kgs import HelmOperator
from kgs import K8SManifest
from kgs import K8SOperator
from kgs import load_recursively
from kgs import Manifest

# simply support bytes type in yaml
yaml.SafeLoader.add_constructor("!bytes", lambda _1, n: n.value.encode())


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

    @mock.patch("kgs.cmd_exec")
    def test_operator_get_state(self, cmd_exec):
        testdata = self.testdata["test_operator_get_state"]
        cmd_exec.return_value = (b"{}", b"{}", 0)

        for td in testdata:
            with self.subTest(td["desc"]):
                oper = K8SOperator()
                cmd_exec.reset_mock()
                manifest = K8SManifest.parse_dict(td["in"])
                ret = oper.get_state(manifest)
                if "expect" in td and td.get("expect"):
                    cmd_exec.assert_called_with(td["expect"])
                if "return" in td:
                    self.assertEqual(td["return"], ret.detail)

    @mock.patch("kgs.cmd_exec")
    def test_operator_create_or_update(self, cmd_exec):
        testdata = self.testdata["test_operator_create_or_update"]

        for td in testdata:
            with self.subTest(td["desc"]):
                cmd_exec.reset_mock()
                cmd_exec.side_effect = iter(td["cmd_exec"])

                oper = K8SOperator()
                manifest = K8SManifest.parse_dict(td["in"])
                oper.create_or_update(manifest, dry_run=False)

                if "expect" in td and td.get("expect"):
                    calls = [{"args": list(l.args), "kwargs": l.kwargs} for l in cmd_exec.call_args_list]
                    assert td["expect"] == calls


class TestHelmOperator(unittest.TestCase):
    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_kgs.yaml")
        with open(path, "r") as f:
            self.testdata = yaml.safe_load(f)

    @mock.patch("kgs.cmd_exec")
    def test_operator_get_state(self, cmd_exec):
        testdata = self.testdata["test_operator_get_state_helm"]

        for td in testdata:
            with self.subTest(td["desc"]):
                cmd_exec.reset_mock()
                cmd_exec.side_effect = iter(td["cmd_exec"])

                oper = HelmOperator()
                manifest = HelmManifest.parse_dict(td["in"])
                ret = oper.get_state(manifest)
                if "expect" in td and td.get("expect"):
                    calls = [{"args": list(l.args), "kwargs": l.kwargs} for l in cmd_exec.call_args_list]
                    assert td["expect"] == calls
                if "return" in td:
                    self.assertEqual(td["return"]["result"], ret.result)
                    self.assertEqual(td["return"]["detail"], ret.detail)


class TestManifestLoader(unittest.TestCase):
    def setUp(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_kgs.yaml")
        with open(path, "r") as f:
            self.testdata = yaml.safe_load(f)

    @mock.patch("kgs.open")
    @mock.patch("kgs.Path")
    def test_recursive_load(self, path_mock, open_mock):
        testdata = self.testdata["test_recursive_load"]

        for td in testdata:
            # mock open
            def open_mock_func(td, f):
                content = td["files"][f]
                file_object = mock_open(read_data=content).return_value
                file_object.__iter__.return_value = content.splitlines(True)
                return file_object

            open_mock.side_effect = partial(open_mock_func, td)

            # mock Path.glob
            path_mock.return_value.glob.return_value = list(map(Path, td["files"].keys()))
            manifests = load_recursively("")
            for idx, m in enumerate(td["manifests"]):
                expected: Manifest
                if "kind" in m:
                    expected = K8SManifest.parse_dict(m)
                else:
                    expected = HelmManifest.parse_dict(m)
                assert expected == manifests[idx]
