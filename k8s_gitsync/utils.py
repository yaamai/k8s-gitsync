import re
import os
from glob import glob
from subprocess import Popen, PIPE
from pathlib import Path
from .resource import Resource
from . import log

logger = log.getLogger(__name__)


def filter_directory_contains_file(files, pattern):
    pattern_re = re.compile(pattern)
    path_list = list(map(Path, files))
    contains_dir_list = [p for p in path_list if pattern_re.match(p.name)]
    logger.debug(f"list of directories contains charts: {contains_dir_list}")

    result = []
    for path in path_list:
        is_path_contain_file = [contains_dir.parent in path.parents for contains_dir in contains_dir_list]
        logger.debug(f"Checking that the directory contains charts: dir[{path}], charts[{is_path_contain_file}]")
        if not any(is_path_contain_file):
            result.append(str(path))

    return result


def get_manifest_files(repo_dir):
    helm_manifest_pattern = re.compile(r"(.*)\.helm$")
    helm_values_pattern = re.compile(r"(.*)\.values\.ya?ml$")
    k8s_pattern = re.compile(r"(.*)\.ya?ml$")

    def _get_helm_file(files):
        manifest_list = []
        helm_match_list = [(f, helm_manifest_pattern.match(f)) for f in files if helm_manifest_pattern.match(f)]
        values_match_list = [(f, helm_values_pattern.match(f)) for f in files if helm_values_pattern.match(f)]
        for filename, helm_match in helm_match_list:
            d = Resource("helm", filename)
            d.values = [f for f, m in values_match_list if helm_match.group(1) == m.group(1)]
            files.remove(filename)
            for e in d.values:
                files.remove(e)
            manifest_list.append(d)
        return files, manifest_list

    def _get_k8s_file(files):
        manifest_list = []
        k8s_files = filter(k8s_pattern.match, files)
        manifest_list.extend([Resource("k8s", f) for f in k8s_files])
        for e in k8s_files:
            files.remove(e)
        return files, manifest_list

    path = os.path.join(repo_dir, "**/*")
    logger.info(f"begin to walk manifest from {path}")
    files = glob(path, recursive=True)
    logger.info(f"  target files:")
    for filepath in files:
        logger.info(f"    {filepath}")

    files = filter_directory_contains_file(files, "Chart\\.yaml")

    files[:], helm_manifest = _get_helm_file(files)
    files[:], k8s_manifest = _get_k8s_file(files)

    logger.info(f"detected k8s manifest files:")
    for m in map(lambda x: x.manifest, k8s_manifest):
        logger.info(f"  {m}")
    logger.info(f"detected helm manifest files:")
    for m in map(lambda x: (x.manifest, x.values), helm_manifest):
        logger.info(f"  {m}")

    manifests = []
    manifests.extend(k8s_manifest)
    manifests.extend(helm_manifest)

    return manifests


def cmd_exec(cmd, stdin=None):
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    outs, errs = p.communicate(stdin)
    log.command_result_debug(logger, cmd, outs, errs)
    return outs, errs, p.returncode
