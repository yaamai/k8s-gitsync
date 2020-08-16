from dataclasses import dataclass
from dataclasses import field
from typing import List
from typing import Type

import yaml
from dataclasses_json import DataClassJsonMixin

from kgs.common import Manifest
from kgs.utils import _safe_get


@dataclass
class _HelmChart(DataClassJsonMixin):
    data: dict = field(default_factory=dict, repr=False)
    name: str = field(init=False)
    version: str = field(init=False)
    repo: str = field(init=False)
    localpath: str = field(init=False)


class HelmChart(_HelmChart):
    @property
    def name(self) -> str:  # type:ignore
        return _safe_get(self.data, "name")

    @name.setter
    def name(self, value: str):  # type:ignore
        self.name = value

    @property
    def version(self) -> str:  # type:ignore
        return _safe_get(self.data, "version")

    @property
    def repo(self) -> str:  # type:ignore
        return _safe_get(self.data, "repo")

    @property
    def localpath(self) -> str:  # type:ignore
        return _safe_get(self.data, "localpath")


@dataclass
class _HelmManifest(Manifest, DataClassJsonMixin):
    data: dict = field(default_factory=dict, repr=False)
    chart: HelmChart = HelmChart({})
    values: dict = field(default_factory=dict, repr=False)
    namespace: str = field(init=False)
    name: str = field(init=False)


# To clearly declare property on dataclass,
# define field on parent, overwrite by property in child
class HelmManifest(_HelmManifest):
    def get_id(self) -> str:
        return f'helm.{self.data["namespace"]}.{self.data["name"]}'

    @property
    def name(self) -> str:  # type:ignore
        return _safe_get(self.data, "name")

    @property
    def namespace(self) -> str:  # type:ignore
        return _safe_get(self.data, "namespace")

    @property
    def chart(self) -> HelmChart:  # type:ignore
        return HelmChart(data=_safe_get(self.data, "chart"))

    def get_values(self) -> dict:
        return self.values

    @classmethod
    def parse_dict(cls: Type["HelmManifest"], d: dict) -> "Manifest":
        m = d.get("manifest", {})
        return HelmManifest(data=m, values=d.get("values", {}), chart=m.get("chart", {}))

    @classmethod
    def parse_file(cls: Type["HelmManifest"], helm_file: str, values_files: List[str]) -> List["Manifest"]:
        d: dict = {"manifest": {}, "values": {}}

        with open(helm_file) as f:
            d["manifest"].update(yaml.safe_load(f))
        for values_file in values_files:
            with open(values_file) as f:
                d["values"].update(yaml.safe_load(f))

        return [cls.parse_dict(d)]
