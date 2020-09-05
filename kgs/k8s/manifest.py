import hashlib
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Type

import yaml
from dataclasses_json import CatchAll
from dataclasses_json import dataclass_json
from dataclasses_json import DataClassJsonMixin
from dataclasses_json import Undefined

from kgs.common import Manifest
from kgs.consts import KGS_DEFAULT_NS
from kgs.consts import KGS_MANAGED_KEY
from kgs.consts import LAST_APPLIED_KEY


@dataclass_json(undefined=Undefined.INCLUDE)
@dataclass
class K8SMetadata:
    name: str
    others: CatchAll = field(repr=False)
    namespace: str = KGS_DEFAULT_NS


# To clearly declare property on dataclass,
# define field on parent, overwrite by property in child
@dataclass_json(undefined=Undefined.INCLUDE)
@dataclass
class K8SManifestBase(Manifest, DataClassJsonMixin):
    kind: str
    metadata: K8SMetadata
    others: CatchAll = field(repr=False)


class K8SManifest(K8SManifestBase):
    def get_id(self):
        namespace = self.metadata.namespace
        return f"{self.kind.lower()}.{namespace}.{self.metadata.name}"

    # override dataclass-json to_dict to update annotated manifest
    def to_dict(self, encode_json=False) -> Dict[str, Any]:
        d = super().to_dict(encode_json=encode_json)
        return K8SManifest._annotate_manifest_data(d)

    @classmethod
    def parse_dict(cls: Type["K8SManifest"], d: dict) -> "K8SManifest":
        return cls.from_dict(d)

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
    def parse_array_of_dict(cls: Type["K8SManifest"], ary: List[dict]) -> List["Manifest"]:
        return [cls.parse_dict(elm) for elm in ary]

    @classmethod
    def parse_file(cls: Type["K8SManifest"], filepath: str) -> List["Manifest"]:
        with open(filepath) as f:
            # some k8s manifest file has empty document
            documents = filter(lambda d: d is not None and isinstance(d, dict), yaml.safe_load_all(f))
            return K8SManifest.parse_array_of_dict(list(documents))
        return []
