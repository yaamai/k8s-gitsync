import hashlib
from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Type

import yaml
from dataclasses_json import DataClassJsonMixin

from kgs.common import Manifest
from kgs.consts import KGS_DEFAULT_NS
from kgs.consts import KGS_MANAGED_KEY
from kgs.consts import LAST_APPLIED_KEY
from kgs.utils import _safe_get


@dataclass
class _K8SManifest(Manifest, DataClassJsonMixin):
    data: dict = field(default_factory=dict, repr=False)
    namespace: str = field(init=False)
    kind: str = field(init=False)
    name: str = field(init=False)


# To clearly declare property on dataclass,
# define field on parent, overwrite by property in child
class K8SManifest(_K8SManifest):
    @property
    def namespace(self) -> str:  # type:ignore
        return _safe_get(self.data, "metadata", "namespace", default=KGS_DEFAULT_NS)

    @property
    def name(self) -> str:  # type:ignore
        return _safe_get(self.data, "metadata", "name")

    @property
    def kind(self) -> str:  # type:ignore
        return _safe_get(self.data, "kind")

    def get_id(self):
        namespace = self.data.get("namespace", KGS_DEFAULT_NS)
        return f'{self.data["kind"].lower()}.{namespace}.{self.data["metadata"]["name"]}'

    @staticmethod
    def _annotate_manifest_data(data: dict) -> dict:
        hashhex = hashlib.sha256(yaml.dump(data).encode()).hexdigest()
        if "metadata" not in data:
            data["metadata"] = {}
        if "annotations" not in data["metadata"]:
            data["metadata"]["annotations"] = {}
        data["metadata"]["annotations"][LAST_APPLIED_KEY] = hashhex
        if "labels" not in data["metadata"]:
            data["metadata"]["labels"] = {}
        data["metadata"]["labels"][KGS_MANAGED_KEY] = "true"

        return data

    @classmethod
    def parse_dict(cls: Type["K8SManifest"], d: dict) -> "Manifest":
        return K8SManifest(data=cls._annotate_manifest_data(d))

    @classmethod
    def parse_array_of_dict(cls: Type["K8SManifest"], ary: List[dict]) -> List["Manifest"]:
        return [cls.parse_dict(elm) for elm in ary]

    @classmethod
    def parse_file(cls: Type["K8SManifest"], filepath: str) -> List["Manifest"]:
        with open(filepath) as f:
            # some k8s manifest file has empty document
            documents = yaml.safe_load_all(f)
            manifests = []
            for document in documents:
                if document is None or not isinstance(document, dict):
                    continue
                manifests.append(cls.parse_dict(document))
        return manifests
