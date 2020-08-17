import unittest
from functools import partial
from pathlib import Path
from unittest import mock
from unittest.mock import mock_open

from kgs.common import Manifest
from kgs.loader import load_recursively
from kgs.manifests.helm import HelmManifest
from kgs.manifests.k8s import K8SManifest
from tests.utils import load_testdata


class TestManifestLoader(unittest.TestCase):
    def setUp(self):
        self.testdata = load_testdata(__file__)

    @mock.patch("builtins.open")
    @mock.patch("kgs.loader.Path")
    def test_recursive_load(self, path_mock, open_mock):
        testdata = self.testdata["test_recursive_load"]

        for td in testdata:
            with self.subTest(td["desc"]):
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
                        expected = K8SManifest.from_dict(m)
                    else:
                        expected = HelmManifest.from_dict(m)
                    assert expected == manifests.result[idx]
