from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Optional
from typing import Type

import yaml
from dataclasses_json import DataClassJsonMixin

from kgs.common import Manifest
from kgs.consts import KGS_DEFAULT_NS


@dataclass
class HelmChart(DataClassJsonMixin):
    name: str
    version: str
    repo: Optional[str] = None
    localpath: Optional[str] = None


@dataclass
class HelmManifest(Manifest, DataClassJsonMixin):
    name: str
    chart: HelmChart = field(repr=False)
    values: dict = field(repr=False)
    namespace: str = KGS_DEFAULT_NS

    def get_id(self) -> str:
        return f"helm.{self.namespace}.{self.name}"

    @classmethod
    def parse_dict(cls: Type["HelmManifest"], d: dict) -> "HelmManifest":
        return cls.from_dict(d)

    @classmethod
    def parse_file(cls: Type["HelmManifest"], helm_file: str, values_files: List[str]) -> List["Manifest"]:
        d: dict = {"values": {}}

        with open(helm_file) as f:
            d.update(yaml.safe_load(f))
        for values_file in values_files:
            with open(values_file) as f:
                d["values"].update(yaml.safe_load(f))

        return [cls.parse_dict(d)]