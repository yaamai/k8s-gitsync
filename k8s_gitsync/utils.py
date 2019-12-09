import re
import os
from subprocess import Popen, PIPE
from .resource import Resource
from . import log

logger = log.getLogger(__name__)


def get_manifest_files(repo_dir):
    helm_manifest_pattern = re.compile(r"(.*)\.helm$")
    helm_values_pattern = re.compile(r"(.*)\.values\.ya?ml$")
    k8s_pattern = re.compile(r"(.*)\.ya?ml$")

    def _get_helm_file(root, files):
        helm_files = []
        helm_files_flat = []
        match_list = [(helm_manifest_pattern.match(f), helm_values_pattern.match(f), f) for f in files]

        for helm_match, values_match, filename in match_list:
            if helm_match:
                d = Resource("helm", os.path.join(root, filename))
                d.values = [
                    os.path.join(root, f)
                    for (_, m, f) in match_list
                    if m is not None and helm_match.group(1) == m.group(1)
                ]
                helm_files.append(d)
                helm_files_flat.append(d["manifest"])
                helm_files_flat.extend(d["values"])

        return helm_files_flat, helm_files

    def _get_k8s_file(root, files):
        k8s_files = [f for f in files if k8s_pattern.match(f)]
        return [Resource("k8s", os.path.join(root, f)) for f in k8s_files]

    logger.info("begin to walk manifest directory.")
    helm_manifest = []
    k8s_manifest = []
    for root, dirs, files in os.walk(repo_dir):
        logger.debug(f"{root}, {dirs}, {files}")

        helm_files_flat, helm_files = _get_helm_file(root, files)
        helm_manifest.extend(helm_files)
        files = set(files) - set(helm_files_flat)

        k8s_manifest.extend(_get_k8s_file(root, files))

    logger.info(f"detected k8s manifest files: {k8s_manifest}")
    logger.info(f"detected helm manifest files: {helm_manifest}")

    manifests = []
    manifests.extend(k8s_manifest)
    manifests.extend(helm_manifest)

    return manifests


def cmd_exec(cmd, stdin=None):
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    outs, errs = p.communicate(stdin)
    log.command_result_debug(logger, cmd, outs, errs)
    return outs, errs, p.returncode
