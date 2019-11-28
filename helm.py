import os
import re
import subprocess
import json
from subprocess import PIPE
from pprint import pprint as pp
import hashlib


def _get_helm_values_hash(release_name):
    cmd_output = subprocess.run([
        "./linux-amd64/helm", "get", "values", release_name, "--output", "json"], stdout=PIPE, stderr=PIPE)
    values = json.loads(cmd_output.stdout.decode())
    pp(values)
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


def _get_helm_release_list():
    cmd_output = subprocess.run(["./linux-amd64/helm", "list", "--output", "json"], stdout=PIPE, stderr=PIPE)
    release_list = json.loads(cmd_output.stdout.decode())['Releases']
    return release_list


def get_helm_state():
    release_list = _get_helm_release_list()
    pp(release_list)
    state = {}
    for e in release_list:
        state_id_str = f'helm.{e["Namespace"]}.{e["Name"]}'
        state[state_id_str] = {
            'chart': e["Chart"],
            'values_hash': _get_helm_values_hash(e['Name'])
        }
    pp(state)


def get_deploy_config():
    helm_deploy_config_pattern = re.compile(r'.*.helm')
    helm_values_pattern = re.compile(r'.helm$')

    def _get_helm_file(files):
        ret = []
        for f in files:
            if helm_deploy_config_pattern.match(f):
                ret.append(f)

                for values_file_pattern in ['.values.yaml', '.values.yml']:
                    values_file_path = helm_values_pattern.sub(values_file_pattern, f)
                    if values_file_path in files:
                        ret.append(values_file_path)
        return ret

    helm_deploy_config = {}
    k8s_deploy_config = {}
    for root, dirs, files in os.walk('repo'):
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


def main():
    # get_helm_state()
    get_deploy_config()


if __name__ == '__main__':
    main()
