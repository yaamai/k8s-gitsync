import re
from collections import OrderedDict
from pathlib import Path
from typing import Callable
from typing import Iterable
from typing import List
from typing import Optional

from kgs.common import Manifest
from kgs.manifests.helm import HelmManifest
from kgs.manifests.k8s import K8SManifest

HELM_MANIFEST_FILE_PATTERN: re.Pattern = re.compile(r"(.*)\.helm$")
HELM_MANIFEST_VALUES_FILE_PATTERN: re.Pattern = re.compile(r"(.*)\.values\.ya?ml$")


def _get_files_in_samedir(paths: Iterable[Path], filepath: Path, condition: Callable[[str], bool]) -> List[Path]:
    dir_files = [p for p in paths if list(p.parents) == list(filepath.parents)]
    return [p for p in dir_files if condition(str(p))]


def _get_files_in_samedir_pattern(paths: Iterable[Path], filepath: Path, pattern: re.Pattern, name: str) -> List[Path]:
    def _(path: str) -> bool:
        m = pattern.match(path)
        return m is not None and m.group(1) == name

    return _get_files_in_samedir(paths, filepath, _)


def _unwrap_any(*args: Optional[re.Match]) -> re.Match:
    for opt in args:
        if opt:
            return opt
    # this function expect least one of args must have value
    raise Exception()


def _try_parse_helm(filepath: Path, paths: "OrderedDict[Path, Optional[bool]]") -> Optional[List[HelmManifest]]:
    match_helm = HELM_MANIFEST_FILE_PATTERN.match(str(filepath))
    match_values = HELM_MANIFEST_VALUES_FILE_PATTERN.match(str(filepath))
    if not match_helm and not match_values:
        return None

    filename = _unwrap_any(match_helm, match_values).group(1)
    values_files = _get_files_in_samedir_pattern(paths, filepath, HELM_MANIFEST_VALUES_FILE_PATTERN, filename)
    helm_file: Path = filepath
    if match_values:
        # rescan helm file based on values file's name
        helm_file_candidate = _get_files_in_samedir_pattern(paths, filepath, HELM_MANIFEST_FILE_PATTERN, filename)
        if len(helm_file_candidate) != 1:
            # when only values file found, skip related files
            for val in values_files:
                paths[val] = False
            return []
        helm_file = helm_file_candidate[0]

    paths[helm_file] = True
    for val in values_files:
        paths[val] = True
    return HelmManifest.parse_file(str(helm_file), [str(p) for p in values_files])


K8S_MANIFEST_FILE_PATTERN: re.Pattern = re.compile(r"(.*)\.ya?ml$")


def _try_parse_k8s(filepath: Path, paths: "OrderedDict[Path, Optional[bool]]") -> Optional[List[K8SManifest]]:
    m = K8S_MANIFEST_FILE_PATTERN.match(str(filepath))
    if not m:
        return None

    manifest = K8SManifest.parse_file(str(filepath))
    paths[filepath] = True
    return manifest


def load_recursively(repo_path: str) -> List[Manifest]:
    # path -> None(not processed), False(skipped), True(processed)
    paths: "OrderedDict[Path, Optional[bool]]" = OrderedDict()
    for path in Path(repo_path).glob("**/*"):
        paths[path] = None

    manifest_list: List[Manifest] = []
    for filepath, processed in paths.items():
        if processed is not None:
            continue

        result = _try_parse_helm(filepath, paths)
        if result is not None:
            manifest_list.extend(result)
            continue

        k8s_result = _try_parse_k8s(filepath, paths)
        if k8s_result is not None:
            manifest_list.extend(k8s_result)
            continue

    return manifest_list
