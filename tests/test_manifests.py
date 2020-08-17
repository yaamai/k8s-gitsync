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
                if "raise" in td:
                    with self.assertRaises(KeyError):
                        K8SManifest.from_dict(td["in"])
                else:
                    K8SManifest.from_dict(td["in"])

    def test_helm_manifest_load(self):
        testdata = self.testdata["test_helm_manifest_load"]
        for td in testdata:
            with self.subTest(td["desc"]):
                if "raise" in td:
                    with self.assertRaises(KeyError):
                        HelmManifest.from_dict(td["in"])
                else:
                    HelmManifest.from_dict(td["in"])
