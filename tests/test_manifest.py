import unittest
from unittest import mock
from unittest.mock import MagicMock
from k8s_gitsync import manifest


class TestManifests(unittest.TestCase):
    @mock.patch('k8s_gitsync.manifest.Path')
    def test_load_recursively(self, path_mock):
        path_mock.return_value = MagicMock()
        manifests = manifest.load_recursively(".")
        self.assertEqual(len(manifests), 0)
