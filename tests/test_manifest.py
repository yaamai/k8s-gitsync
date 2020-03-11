import unittest
from pathlib import Path
from unittest import mock
from unittest.mock import mock_open
from kgs import manifest


class TestManifests(unittest.TestCase):
    def setUp(self):
        self.maxDiff = None

    @mock.patch('kgs.manifest.Path')
    @mock.patch('kgs.manifest.open')
    def test_load_recursively(self, open_mock, path_mock):
        testdata = [
            [
                [],
                [],
                b'{}',
            ],
            [
                [Path("hoge.yaml")],
                [manifest.K8SManifest()],
                b'{}',
            ],
            [
                [Path("hoge.yaml")],
                [manifest.K8SManifest(), manifest.K8SManifest()],
                b'---\na: 100\n---\nb:100',
            ],
            [
                [Path("hoge.helm")],
                [manifest.HelmManifest(manifest_filepath="hoge.helm", values_filepath=[])],
                b'{}',
            ],
            [
                [Path("hoge.helm"), Path("Chart.yaml")],
                [],
                b'{}',
            ],
            [
                [Path("hoge.helm"), Path("chart/Chart.yaml")],
                [manifest.HelmManifest(manifest_filepath="hoge.helm", values_filepath=[])],
                b'{}',
            ],
            [
                [Path("hoge.helm"), Path("hoge.values.yaml")],
                [manifest.HelmManifest(manifest_filepath="hoge.helm", values_filepath=["hoge.values.yaml"])],
                b'{}',
            ],
            [
                [Path("hoge.helm"), Path("hoge.values.yaml"), Path("hoge.dev.values.yaml")],
                [manifest.HelmManifest(
                    manifest_filepath="hoge.helm",
                    values_filepath=["hoge.values.yaml", "hoge.dev.values.yaml"])],
                b'{}',
            ],
        ]

        for paths, expect, data in testdata:
            with self.subTest(paths):
                path_mock.return_value.glob.return_value = paths
                mock_open(open_mock, read_data=data)
                manifests = manifest.load_recursively(".")
                self.assertEqual(expect, manifests)
