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
                [manifest.K8SManifest(filepath='hoge.yaml', data={'metadata': {'annotations': {'k8s-gitsync/last-applied-confighash': 'ca3d163bab055381827226140568f3bef7eaac187cebd76878e0b63e9e442356'}, 'labels': {'k8s-gitsync/managed': 'true'}}})],
                b'{}',
            ],
            [
                [Path("hoge.yaml")],
                [manifest.K8SManifest(filepath='hoge.yaml', data={'a': 100, 'metadata': {'annotations': {'k8s-gitsync/last-applied-confighash': '8e4933317f9bdd87e5ae4e3dd4275e36ec18b3a01b57b920aa91a0f4915a5220'}, 'labels': {'k8s-gitsync/managed': 'true'}}}), manifest.K8SManifest(filepath='hoge.yaml', data={'b': 100, 'metadata': {'annotations': {'k8s-gitsync/last-applied-confighash': '14d2bf305401a8ebc4043abec2d226c9b5b44c4556aaa048d186405712b3530e'}, 'labels': {'k8s-gitsync/managed': 'true'}}})],
                b'---\na: 100\n---\nb: 100',
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
