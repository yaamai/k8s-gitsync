from dataclasses import dataclass
import re
from pathlib import Path
from typing import List, Optional, TypeVar, Type, ClassVar
import yaml
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


@dataclass
class K8SManifest(Manifest):
    file_pattern: ClassVar[re.Pattern] = re.compile(r"(.*)\.ya?ml$")

    filepath: str = ""
    data: Optional[dict] = None

    @classmethod
    def _expand_multi_document_file(cls, filepath: str) -> List['K8SManifest']:
        with open(filepath) as f:
            documents = yaml.safe_load_all(f)
            manifests = []
            for document in documents:
                # some k8s manifest has empty document
                if document is None:
                    continue
                r = K8SManifest()
                manifests.append(r)

        return manifests

    @classmethod
    def load(cls: Type[T], path_list: List[Path]) -> List['K8SManifest']:
        manifest_list: List[K8SManifest] = []
        for path in path_list:
            m = K8SManifest.file_pattern.match(str(path))
            if m:
                manifest_list.extend(K8SManifest._expand_multi_document_file(str(path)))

        return manifest_list


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


def _exclude_directory_contains_file(path_list: List[Path], pattern: re.Pattern) -> List[Path]:
    contains_dir_list = [p for p in path_list if pattern.match(p.name)]

    result = []
    for path in path_list:
        is_path_contain_file = [contains_dir.parent in path.parents for contains_dir in contains_dir_list]
        if not any(is_path_contain_file):
            result.append(path)

    return result


def _exclude_helm_manifest(path_list: List[Path], helm_manifests: List[HelmManifest]) -> List[Path]:
    helm_manifest_file_list: List[str] = []
    for m in helm_manifests:
        helm_manifest_file_list.extend(m.get_filepath())

    return list(filter(lambda p: str(p) not in helm_manifest_file_list, path_list))


def load_recursively(path: str) -> List[Manifest]:
    path_list = list(Path(path).glob("**/*"))

    # exclude helm chart directory
    path_list = _exclude_directory_contains_file(path_list, re.compile("Chart\\.yaml"))

    # load manifests
    helm_manifests = HelmManifest.load(path_list)

    path_list = _exclude_helm_manifest(path_list, helm_manifests)
    k8s_manifests = K8SManifest.load(path_list)

    manifest_list: List[Manifest] = []
    manifest_list.extend(helm_manifests)
    manifest_list.extend(k8s_manifests)
    return manifest_list
