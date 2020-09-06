import re
from collections import OrderedDict
from enum import Enum
from pathlib import Path
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple

import yaml
from toposort import toposort_flatten  # type: ignore

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


def sort_by_dependency(repo, manifests):
    try:
        order = {}
        with open(Path(repo) / "order.yaml") as f:
            order = yaml.safe_load(f)
        for k, v in order.items():
            order[k] = set(v)

        manifest_map = {}
        for m in manifests:
            manifest_map[m.get_id()] = m

        sorted_id_list = toposort_flatten(order)
        sorted_manifests = []
        for manifest_id in sorted_id_list:
            m = manifest_map.pop(manifest_id, None)
            if m:
                sorted_manifests.append(m)
        for (_, m) in manifest_map.items():
            sorted_manifests.append(m)

        return sorted_manifests

    except FileNotFoundError:
        return manifests


def load_recursively(repo_path: str) -> Result[List[Manifest]]:
    # path -> None(not processed), False(skipped), True(processed)
    paths: "OrderedDict[Path, ParseResultKind]" = OrderedDict()
    for path in Path(repo_path).glob("**/*"):
        if path == (Path(repo_path) / "order.yaml"):
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
