from pathlib import Path
import re
from typing import List
from kgs.manifest.common import Manifest
from kgs.manifest.k8s import K8SManifest
from kgs.manifest.helm import HelmManifest


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
