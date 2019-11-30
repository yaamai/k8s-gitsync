import re
import os
import log

logger = log.getLogger(__name__)


def get_deploy_config(repo_dir):
    helm_deploy_config_pattern = re.compile(r".*.helm")
    helm_values_pattern = re.compile(r".helm$")

    def _get_helm_file(files):
        ret = []
        for f in files:
            if helm_deploy_config_pattern.match(f):
                ret.append(f)

                for values_file_pattern in [".values.yaml", ".values.yml"]:
                    values_file_path = helm_values_pattern.sub(values_file_pattern, f)
                    if values_file_path in files:
                        ret.append(values_file_path)
        return ret

    helm_deploy_config = {}
    k8s_deploy_config = {}
    logger.info("begin to walk manifest directory.")
    for root, dirs, files in os.walk(repo_dir):
        logger.debug(root, dirs, files)

        helm_files = _get_helm_file(files)
        if helm_files:
            helm_deploy_config[root] = helm_files

        k8s_files = set(files) - set(helm_files)
        if k8s_files:
            k8s_deploy_config[root] = k8s_files

    logger.info(f"detected k8s manifest files: {k8s_deploy_config}")
    logger.info(f"detected helm manifest files: {helm_deploy_config}")

    return k8s_deploy_config, helm_deploy_config
