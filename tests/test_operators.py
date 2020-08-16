import unittest
from unittest import mock

from kgs.manifests.helm import HelmManifest
from kgs.manifests.k8s import K8SManifest
from kgs.operators.helm import HelmOperator
from kgs.operators.k8s import K8SOperator
from tests.utils import load_testdata


class TestK8SOperator(unittest.TestCase):
    def setUp(self):
        self.testdata = load_testdata(__file__)

    @mock.patch("kgs.utils.cmd_exec")
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

    @mock.patch("kgs.utils.cmd_exec")
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
        self.testdata = load_testdata(__file__)

    @mock.patch("kgs.utils.cmd_exec")
    def test_operator_get_state(self, cmd_exec):
        testdata = self.testdata["test_operator_get_state_helm"]

        for td in testdata:
            with self.subTest(td["desc"]):
                cmd_exec.reset_mock()
                cmd_exec.side_effect = iter(td["cmd_exec"])

                oper = HelmOperator()
                manifest = HelmManifest.from_dict(td["in"])
                ret = oper.get_state(manifest)
                if "expect" in td and td.get("expect"):
                    calls = [{"args": list(l.args), "kwargs": l.kwargs} for l in cmd_exec.call_args_list]
                    assert td["expect"] == calls
                if "return" in td:
                    self.assertEqual(td["return"]["result"], ret.result)
                    self.assertEqual(td["return"]["detail"], ret.detail)
