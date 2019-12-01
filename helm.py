import re
import yaml
import json
import hashlib
import utils
import log

logger = log.getLogger(__name__)


def _calc_helm_values_hash(values_dict):
    return hashlib.sha256(json.dumps(values_dict, sort_keys=True).encode()).hexdigest()


def _get_helm_values_hash(release_name):
    cmd = ["./helm2/helm", "get", "values", release_name, "--output", "json"]
    outs, _, _ = utils.cmd_exec(cmd)
    values = json.loads(outs.decode())
    return _calc_helm_values_hash(values)


def _get_helm_release_list():
    cmd = ["./helm2/helm", "list", "--output", "json"]
    outs, _, _ = utils.cmd_exec(cmd)
    if not outs:
        return []
    release_list = json.loads(outs.decode())
    return release_list["Releases"]


def _upgrade_or_install_helm_release(release_name, namespace, version, chart_name, values):
    cmd = ["./helm2/helm", "upgrade",
           "--output", "json",
           "--install", release_name,
           "--namespace", namespace,
           "--values", "-",
           "--version", version,
           chart_name]
    outs, _, _ = utils.cmd_exec(cmd, values)

    # remove WARNING:
    warning_log_re = re.compile(r'^(WARNING:|DEBUG:|Release).*', re.MULTILINE)
    outs_json = warning_log_re.sub("", outs.decode())

    return json.loads(outs_json)


def get_helm_state():
    release_list = _get_helm_release_list()
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
        values = yaml.safe_load(open(f'{directory}/{manifest_files["values"][0]}'))

        # helm consider null values to {}
        if values is None:
            values = {}

        id_str = f'helm.{manifest["namespace"]}.{manifest["name"]}'
        manifest_dict[id_str] = {
            "chart": f'{manifest["chart"]["name"]}-{manifest["chart"]["version"]}',
            "values_hash": _calc_helm_values_hash(values),
            "_manifest_data": manifest,
            "_values_data": values
        }

    return manifest_dict


def _safe_get(d, *args):
    r = d
    for k in args:
        if k not in r:
            return "[UNKNOWN]"
        r = r[k]
    return r


def _hash_head(s):
    if s == "[UNKNOWN]":
        return s

    return s[:8]


def compare_state_and_manifest(state_dict, manifest_dict):
    for id_str, manifest in manifest_dict.items():
        logger.debug(f'Checking helm releases ...')
        logger.debug(f'  {id_str}:')
        logger.debug(f'    not installed: {id_str not in state_dict}')
        logger.debug(f'    chart ver    : {manifest["chart"]} <-> {_safe_get(state_dict, id_str, "chart")}')
        logger.debug(f'    values hash  : {_hash_head(manifest["values_hash"])}'
                     f' <-> {_hash_head(_safe_get(state_dict, id_str, "values_hash"))}')
        if (id_str not in state_dict or
                manifest['chart'] != state_dict[id_str]['chart'] or
                manifest['values_hash'] != state_dict[id_str]['values_hash']):
            yield id_str, manifest['_manifest_data'], manifest['_values_data']


def main():
    state_dict = get_helm_state()
    manifest_dict = get_helm_manifest()
    for id_str, manifest, values in compare_state_and_manifest(state_dict, manifest_dict):
        _upgrade_or_install_helm_release(
            manifest['name'],
            manifest['namespace'],
            manifest['chart']['version'],
            manifest['chart']['name'],
            yaml.safe_dump(values).encode())


if __name__ == "__main__":
    main()
