import re
import os


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
    for root, dirs, files in os.walk(repo_dir):
        print(root, dirs, files)

        helm_files = _get_helm_file(files)
        if helm_files:
            helm_deploy_config[root] = helm_files

        k8s_files = set(files) - set(helm_files)
        if k8s_files:
            k8s_deploy_config[root] = k8s_files

    print(helm_deploy_config)
    print(k8s_deploy_config)

    return k8s_deploy_config, helm_deploy_config
