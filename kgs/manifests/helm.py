from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Type

import yaml
from dataclasses_json import dataclass_json

from kgs.manifests.common import Manifest
from kgs.utils import _safe_get


@dataclass_json
@dataclass
class HelmManifest(Manifest):
    data: dict = field(default_factory=dict)  # , repr=False)
    values: dict = field(default_factory=dict)  # , repr=False)

    def get_name(self) -> str:
        return _safe_get(self.data, "name")

    def get_namespace(self) -> str:
        return _safe_get(self.data, "namespace")

    def get_chart(self) -> str:
        return _safe_get(self.data, "chart")

    @classmethod
    def parse_dict(cls: Type["HelmManifest"], d: dict) -> "HelmManifest":
        return HelmManifest(data=d["manifest"], values=d["values"])

    @classmethod
    def parse_file(cls: Type["HelmManifest"], helm_file: str, values_files: List[str]) -> List["HelmManifest"]:
        d: dict = {"manifest": {}, "values": {}}

        with open(helm_file) as f:
            d["manifest"].update(yaml.safe_load(f))
        for values_file in values_files:
            with open(values_file) as f:
                d["values"].update(yaml.safe_load(f))

        return [cls.parse_dict(d)]
