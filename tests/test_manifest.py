import os
import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import mock_open
import yaml
from kgs.manifest import load_recursively, K8SManifest, HelmManifest


class TestManifests(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    @mock.patch('kgs.manifest.Path')
    @mock.patch('kgs.manifest.open')
    def test_load_recursively(self, open_mock, path_mock):
        testdata_list = []
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "test_manifest_data.yaml")
        with open(path, "r") as f:
            testdata_list = yaml.safe_load(f)

        data = {}
        for data in testdata_list:
            with self.subTest(data["files"].keys()):
                def open_mock_func(f):
                    content = data["files"][f]
                    file_object = mock_open(read_data=content).return_value
                    file_object.__iter__.return_value = content.splitlines(True)
                    return file_object

                path_mock.return_value.glob.return_value = list(map(Path, data["files"].keys()))
                open_mock.side_effect = open_mock_func

                expect = []
                for m in data["manifests"]:
                    if "type" not in m:
                        continue

                    typ = m["type"]
                    if typ == "k8s":
                        expect.append(K8SManifest.from_dict(m))
                    elif typ == "helm":
                        expect.append(HelmManifest.from_dict(m))

                manifests = load_recursively(".")
                self.assertEqual(expect, manifests)
