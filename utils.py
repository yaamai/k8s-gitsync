import re
import os
from subprocess import Popen, PIPE
import log

logger = log.getLogger(__name__)


def get_manifest_files(repo_dir):
    helm_manifest_pattern = re.compile(r'.*.helm')
    helm_values_pattern = re.compile(r'.helm$')

    def _get_helm_file(files):
        ret = []
        for f in files:
            if helm_manifest_pattern.match(f):
                ret.append(f)

                for values_file_pattern in [".values.yaml", ".values.yml"]:
                    values_file_path = helm_values_pattern.sub(values_file_pattern, f)
                    if values_file_path in files:
                        ret.append(values_file_path)
        return ret

    logger.info("begin to walk manifest directory.")
    helm_manifest = {}
    k8s_manifest = {}
    for root, dirs, files in os.walk(repo_dir):
        logger.debug(f"{root}, {dirs}, {files}")

        helm_files = _get_helm_file(files)
        if helm_files:
            helm_manifest[root] = helm_files

        k8s_files = set(files) - set(helm_files)
        if k8s_files:
            k8s_manifest[root] = k8s_files

    logger.info(f"detected k8s manifest files: {k8s_manifest}")
    logger.info(f"detected helm manifest files: {helm_manifest}")

    return k8s_manifest, helm_manifest


def cmd_exec(cmd, stdin=None):
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    outs, errs = p.communicate(stdin)
    log.command_result_debug(logger, cmd, outs, errs)
    return outs, errs, p.returncode
