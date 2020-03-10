import re
from pathlib import Path
from typing import List, Optional, TypeVar, Type
from typing_extensions import Protocol


T = TypeVar('T', bound='Manifest')


class Manifest(Protocol):
    def get_id(self) -> str:
        ...

    def get_filepath(self) -> List[str]:
        ...

    @classmethod
    def load(cls: Type[T], path_list: List[Path]) -> List[T]:
        ...


class HelmManifest(Manifest):
    namespace: str
    release_name: str
    repo: Optional[str]
    localpath: Optional[str]
    chart_name: str
    chart_version: str
    values: dict

    @classmethod
    def load(cls: 'HelmManifest', path_list: List[Path]) -> List['HelmManifest']:
        return ""

    def get_id(self) -> str:
        return f'helm.{self.namespace}.{self.release_name}'


def _exclude_directory_contains_file(path_list: List[Path], pattern: re.Pattern) -> List[Path]:
    contains_dir_list = [p for p in path_list if pattern.match(p.name)]

    result = []
    for path in path_list:
        is_path_contain_file = [contains_dir.parent in path.parents for contains_dir in contains_dir_list]
        if not any(is_path_contain_file):
            result.append(path)

    return result


def load_recursively(path: str) -> List[Manifest]:
    path_list = list(Path(path).glob("**/*"))

    # exclude helm chart directory
    path_list = _exclude_directory_contains_file(path_list, re.compile("Chart\\.yaml"))

    # load manifests
    manifest_list = []
    manifest_list.extend(HelmManifest.load(path_list))

    return manifest_list
