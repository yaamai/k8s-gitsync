from pathlib import Path
from typing import List, ClassVar
from dataclasses import dataclass, field
import hashlib
import re
import yaml
from dataclasses_json import dataclass_json
from kgs.manifest.common import Manifest
from kgs.consts import KGS_DEFAULT_NS, LAST_APPLIED_KEY, KGS_MANAGED_KEY


@dataclass_json
@dataclass
class K8SManifest(Manifest):
    file_pattern: ClassVar[re.Pattern] = re.compile(r"(.*)\.ya?ml$")

    filepath: str = ""
    data: dict = field(default_factory=dict)

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

    @staticmethod
    def _expand_multi_document_file(filepath: str) -> List['K8SManifest']:
        with open(filepath) as f:
            documents = yaml.safe_load_all(f)
            manifests = []
            for document in documents:
                # some k8s manifest has empty document
                if document is None or not isinstance(document, dict):
                    continue

                document = K8SManifest._annotate_manifest_data(document)

                r = K8SManifest(filepath=filepath, data=document)
                manifests.append(r)

        return manifests

    @classmethod
    def load(cls, path_list: List[Path]) -> List['K8SManifest']:
        manifest_list: List[K8SManifest] = []
        for path in path_list:
            m = K8SManifest.file_pattern.match(str(path))
            if m:
                manifest_list.extend(K8SManifest._expand_multi_document_file(str(path)))

        return manifest_list

    def get_id(self):
        namespace = self.data.get("namespace", KGS_DEFAULT_NS)
        return f'{self.data["kind"].lower()}.{namespace}.{self.data["metadata"]["name"]}'
