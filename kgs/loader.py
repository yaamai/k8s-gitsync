import re
from collections import OrderedDict
from enum import Enum
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import yaml

from kgs import utils
from kgs.common import Manifest
from kgs.helm.manifest import HelmManifest
from kgs.k8s.manifest import K8SManifest
from kgs.result import Result

HELM_MANIFEST_FILE_PATTERN: re.Pattern = re.compile(r"(.*)\.helm$")
HELM_MANIFEST_VALUES_FILE_PATTERN: re.Pattern = re.compile(r"(.*)\.values\.ya?ml$")


class ParseResultKind(Enum):
    processed = "processed"
    not_processed = "not_processed"
    skipped = "skipped"


ParseResult = Tuple[Optional[List[Manifest]], List[Tuple[Path, ParseResultKind]]]


def _try_parse_helm(filepath: Path, paths: Iterable[Path]) -> ParseResult:
    match_helm = HELM_MANIFEST_FILE_PATTERN.match(str(filepath))
    match_values = HELM_MANIFEST_VALUES_FILE_PATTERN.match(str(filepath))
    if not match_helm and not match_values:
        return (None, [])

    filename = utils.unwrap_any(match_helm, match_values).group(1)
    values_files = utils.get_files_in_samedir_pattern(paths, filepath, HELM_MANIFEST_VALUES_FILE_PATTERN, filename)
    helm_file: Path = filepath
    if match_values:
        # rescan helm file based on values file's name
        helm_file_candidate = utils.get_files_in_samedir_pattern(paths, filepath, HELM_MANIFEST_FILE_PATTERN, filename)
        if len(helm_file_candidate) != 1:
            # when only values file found, skip related files
            return ([], [(p, ParseResultKind.skipped) for p in values_files])
        helm_file = helm_file_candidate[0]

    manifest = HelmManifest.parse_file(str(helm_file), [str(p) for p in values_files])
    files = [(helm_file, ParseResultKind.processed)] + [(p, ParseResultKind.processed) for p in values_files]
    return manifest, files


K8S_MANIFEST_FILE_PATTERN: re.Pattern = re.compile(r"(.*)\.ya?ml$")


def _try_parse_k8s(filepath: Path, _) -> ParseResult:
    m = K8S_MANIFEST_FILE_PATTERN.match(str(filepath))
    if not m:
        return (None, [])

    manifest = K8SManifest.parse_file(str(filepath))
    return (manifest, [(filepath, ParseResultKind.processed)])


def _update_result(paths: "OrderedDict[Path, ParseResultKind]", files: List[Tuple[Path, ParseResultKind]]):
    for (path, kind) in files:
        paths[path] = kind


def _load_depends_data(repo, manifests) -> Dict[str, List[str]]:
    # defaults to no dependency
    depends_data: Dict[str, List[str]] = {m.get_id(): [] for m in manifests}
    try:
        with open(Path(repo) / "depends.yaml") as f:
            depends_data |= yaml.safe_load(f)
    except FileNotFoundError:
        pass

    return depends_data


def _expand_dependency_dict(depends_data: dict) -> Dict[str, Set]:
    # <id>: <dependency-id>
    # <id>: [<dependency-id>, ...]
    # <id>: [{"by": <required-by-id>}, ...]
    depends_map: Dict[str, Set] = {}

    def _update_set(key, dat):
        if key not in depends_map:
            depends_map[key] = set()
        depends_map[key] |= dat

    for k, depends_list in depends_data.items():
        if k not in depends_map:
            depends_map[k] = set([])

        if isinstance(depends_list, str):
            _update_set(k, set([depends_list]))

        for depends in depends_list:
            if isinstance(depends, str):
                _update_set(k, set([depends]))
            if isinstance(depends, dict):
                if "on" in depends:
                    _update_set(k, set([depends["on"]]))
                if "by" in depends:
                    for d in depends["by"]:
                        _update_set(d, set([k]))
    return depends_map


def get_topo_sorter(repo, manifests) -> TopologicalSorter:
    depends_data = _load_depends_data(repo, manifests)
    depends_map = _expand_dependency_dict(depends_data)

    return TopologicalSorter(depends_map)


def sorted_manifests(repo, manifests):
    sorter = get_topo_sorter(repo, manifests)

    manifest_map = {}
    for m in manifests:
        manifest_map[m.get_id()] = m
    for manifest_id in sorter.static_order():
        yield manifest_map.get(manifest_id)


def load_recursively(repo_path: str) -> Result[List[Manifest]]:
    # path -> None(not processed), False(skipped), True(processed)
    paths: "OrderedDict[Path, ParseResultKind]" = OrderedDict()
    for path in Path(repo_path).glob("**/*"):
        if path == (Path(repo_path) / "depends.yaml"):
            continue
        paths[path] = ParseResultKind.not_processed

    manifest_list: List[Manifest] = []
    for filepath, processed in paths.items():
        if processed != ParseResultKind.not_processed:
            continue

        manifests, files = _try_parse_helm(filepath, paths)
        _update_result(paths, files)
        if manifests is not None:
            manifest_list.extend(manifests)
            continue

        manifests, files = _try_parse_k8s(filepath, paths)
        _update_result(paths, files)
        if manifests is not None:
            manifest_list.extend(manifests)
            continue

    return Result.ok(manifest_list, {"paths": paths})
