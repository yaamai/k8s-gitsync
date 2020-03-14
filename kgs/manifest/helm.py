from pathlib import Path
from dataclasses import dataclass
from typing import List, ClassVar, Optional
import re
from dataclasses_json import dataclass_json
from kgs.manifest.common import Manifest


@dataclass_json
@dataclass
class HelmManifest(Manifest):
    manifest_pattern: ClassVar[re.Pattern] = re.compile(r"(.*)\.helm$")
    values_pattern: ClassVar[re.Pattern] = re.compile(r"(.*)\.values\.ya?ml$")

    namespace: str = ""
    release_name: str = ""
    repo: Optional[str] = None
    localpath: Optional[str] = None
    chart_name: str = ""
    chart_version: str = ""
    values: Optional[dict] = None
    manifest_filepath: str = ""
    values_filepath: Optional[List[str]] = None

    @classmethod
    def load(cls, path_list: List[Path]) -> List['HelmManifest']:
        manifest_path_list = []
        for path in path_list:
            m = HelmManifest.manifest_pattern.match(str(path))
            if m:
                manifest_path_list.append((path, m))

        values_path_list = []
        for path in path_list:
            m = HelmManifest.values_pattern.match(str(path))
            if m:
                values_path_list.append((path, m))

        manifest_list: List[HelmManifest] = []
        manifest_match: re.Match  # suppress cell var from loop warn by pylint
        for (manifest_path, manifest_match) in manifest_path_list:
            f = filter(lambda v: v[1].group(1).startswith(manifest_match.group(1)), values_path_list)
            values_files = list(map(lambda v: str(v[0]), f))
            manifest = HelmManifest(manifest_filepath=str(manifest_path), values_filepath=values_files)
            manifest_list.append(manifest)

        return manifest_list

    def get_id(self) -> str:
        return f'helm.{self.namespace}.{self.release_name}'

    def get_filepath(self) -> List[str]:
        r = [self.manifest_filepath]
        if self.values_filepath:
            r.extend(self.values_filepath)
        return r
