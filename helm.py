import re
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


def _upgrade_or_install_helm_release(release_name, namespace, version, chart_name, values):
    cmd = ["./linux-amd64/helm", "upgrade",
           "--output", "json",
           "--install", release_name,
           "--namespace", namespace,
           "--values", "-",
           "--version", version,
           chart_name]
    outs, _, _ = utils.cmd_exec(cmd, values)

    # remove WARNING:
    warning_log_re = re.compile(r'^WARNING:.*')
    outs_json = warning_log_re.sub("", outs.decode())

    return json.loads(outs_json)


def get_helm_state():
    release_list = _get_helm_release_list()
    pp(release_list)
    state = {}
    for e in release_list:
        state_id_str = f'helm.{e["Namespace"]}.{e["Name"]}'
        state[state_id_str] = {"chart": e["Chart"], "values_hash": _get_helm_values_hash(e["Name"])}
    return state


def get_helm_manifest():
    _, helm_manifest_files = utils.get_manifest_files("repo")

    manifest_dict = {}
    for directory, manifest_files in helm_manifest_files.items():
        manifest = yaml.safe_load(open(f'{directory}/{manifest_files["manifest"]}'))
        # TODO: currently, only support one values file
        print(open(f'{directory}/{manifest_files["values"][0]}', "rb").read())
        values = yaml.safe_load(open(f'{directory}/{manifest_files["values"][0]}'))
        id_str = f'helm.{manifest["namespace"]}.{manifest["name"]}'
        manifest_dict[id_str] = {
            "chart": f'{manifest["chart"]["name"]}-{manifest["chart"]["version"]}',
            "values_hash": _calc_helm_values_hash(values),
            "_manifest_data": manifest,
            "_values_data": values
        }

    pp(manifest_dict)
    return manifest_dict


def compare_state_and_manifest(state_dict, manifest_dict):
    for id_str, manifest in manifest_dict.items():
        if (id_str not in state_dict or
                manifest['chart'] != state_dict[id_str]['chart'] or
                manifest['values_hash'] != state_dict[id_str]['values_hash']):
            yield id_str, manifest['_manifest_data'], manifest['_values_data']


def main():
    # print(hashlib.sha256(json.dumps({"b": 100, "a": 200}, sort_keys=True).encode()).hexdigest())
    # print(hashlib.sha256(json.dumps({"a": 200, "b": 100}, sort_keys=True).encode()).hexdigest())
    state_dict = get_helm_state()
    manifest_dict = get_helm_manifest()
    for id_str, manifest, values in compare_state_and_manifest(state_dict, manifest_dict):
        _upgrade_or_install_helm_release(
            manifest['name'],
            manifest['namespace'],
            manifest['chart']['version'],
            manifest['chart']['name'],
            values)


if __name__ == "__main__":
    main()
