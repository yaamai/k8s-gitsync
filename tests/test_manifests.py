import unittest

from kgs.manifests.helm import HelmManifest
from kgs.manifests.k8s import K8SManifest
from tests.utils import load_testdata


class TestManifests(unittest.TestCase):
    def setUp(self):
        self.testdata = load_testdata(__file__)

    def test_k8s_manifest_load(self):
        testdata = self.testdata["test_k8s_manifest_load"]
        for td in testdata:
            with self.subTest(td["desc"]):
                actual = K8SManifest.parse_dict(td["in"])
                expect = K8SManifest.from_dict(td["expect"])
                self.assertEqual(expect, actual)

    def test_helm_manifest_load(self):
        testdata = self.testdata["test_helm_manifest_load"]
        for td in testdata:
            with self.subTest(td["desc"]):
                actual = HelmManifest.parse_dict(td["in"])
                expect = HelmManifest.from_dict(td["expect"])
                self.assertEqual(expect, actual)
