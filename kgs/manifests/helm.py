from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Type

import yaml
from dataclasses_json import DataClassJsonMixin

from kgs.common import Manifest
from kgs.utils import _safe_get


@dataclass
class HelmManifest(Manifest, DataClassJsonMixin):
    data: dict = field(default_factory=dict)  # , repr=False)
    values: dict = field(default_factory=dict)  # , repr=False)

    def get_id(self) -> str:
        return f'helm.{self.data["namespace"]}.{self.data["name"]}'

    def get_kind(self) -> str:
        return "helm"

    def get_name(self) -> str:
        return _safe_get(self.data, "name")

    def get_namespace(self) -> str:
        return _safe_get(self.data, "namespace")

    def get_chart(self) -> dict:
        return _safe_get(self.data, "chart")

    def get_values(self) -> dict:
        return self.values

    @classmethod
    def parse_dict(cls: Type["HelmManifest"], d: dict) -> "HelmManifest":
        return HelmManifest(data=d.get("manifest", {}), values=d.get("values", {}))

    @classmethod
    def parse_file(cls: Type["HelmManifest"], helm_file: str, values_files: List[str]) -> List["HelmManifest"]:
        d: dict = {"manifest": {}, "values": {}}

        with open(helm_file) as f:
            d["manifest"].update(yaml.safe_load(f))
        for values_file in values_files:
            with open(values_file) as f:
                d["values"].update(yaml.safe_load(f))

        return [cls.parse_dict(d)]
