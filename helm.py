import yaml
import json
from pprint import pprint as pp
import hashlib
import utils


def _calc_helm_values_hash(values_dict):
    return hashlib.sha256(json.dumps(values_dict, sort_keys=True).encode()).hexdigest()


def _get_helm_values_hash(release_name):
    cmd = ["./linux-amd64/helm", "get", "values", release_name, "--output", "json"]
    outs, _, _ = utils.cmd_exec(cmd)
    values = json.loads(outs.decode())
    return _calc_helm_values_hash(values)


def _get_helm_release_list():
    cmd = ["./linux-amd64/helm", "list", "--output", "json"]
    outs, _, _ = utils.cmd_exec(cmd)
    release_list = json.loads(outs.decode())["Releases"]
    return release_list


def get_helm_state():
    release_list = _get_helm_release_list()
    pp(release_list)
    state = {}
    for e in release_list:
        state_id_str = f'helm.{e["Namespace"]}.{e["Name"]}'
        state[state_id_str] = {"chart": e["Chart"], "values_hash": _get_helm_values_hash(e["Name"])}
    pp(state)


def get_helm_manifest():
    _, helm_manifest_files = utils.get_manifest_files("repo")

    manifest_dict = {}
    for directory, manifest_files in helm_manifest_files.items():
        manifest = yaml.safe_load(open(f'{directory}/{manifest_files["manifest"]}'))
        # TODO: currently, only support one values file
        values = yaml.safe_load(open(f'{directory}/{manifest_files["values"][0]}'))
        id_str = f'helm.{manifest["namespace"]}.{manifest["name"]}'
        manifest_dict[id_str] = {
            "chart": f'{manifest["chart"]["name"]}-{manifest["chart"]["version"]}',
            "values_hash": _calc_helm_values_hash(values)
        }

    pp(manifest_dict)
    return manifest_dict


def main():
    # print(hashlib.sha256(json.dumps({"b": 100, "a": 200}, sort_keys=True).encode()).hexdigest())
    # print(hashlib.sha256(json.dumps({"a": 200, "b": 100}, sort_keys=True).encode()).hexdigest())
    get_helm_state()
    get_helm_manifest()


if __name__ == "__main__":
    main()
