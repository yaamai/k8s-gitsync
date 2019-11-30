import subprocess
import json
from subprocess import PIPE
from pprint import pprint as pp
import hashlib
import utils


def _get_helm_values_hash(release_name):
    cmd_output = subprocess.run(
        ["./linux-amd64/helm", "get", "values", release_name, "--output", "json"], stdout=PIPE, stderr=PIPE
    )
    values = json.loads(cmd_output.stdout.decode())
    pp(values)
    return hashlib.sha256(json.dumps(values).encode()).hexdigest()


def _get_helm_release_list():
    cmd_output = subprocess.run(["./linux-amd64/helm", "list", "--output", "json"], stdout=PIPE, stderr=PIPE)
    release_list = json.loads(cmd_output.stdout.decode())["Releases"]
    return release_list


def get_helm_state():
    release_list = _get_helm_release_list()
    pp(release_list)
    state = {}
    for e in release_list:
        state_id_str = f'helm.{e["Namespace"]}.{e["Name"]}'
        state[state_id_str] = {"chart": e["Chart"], "values_hash": _get_helm_values_hash(e["Name"])}
    pp(state)


def main():
    # get_helm_state()
    utils.get_deploy_config()


if __name__ == "__main__":
    main()
