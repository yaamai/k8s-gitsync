import re
import yaml
import json
import hashlib
import utils
import log

logger = log.getLogger(__name__)

KGS_MANAGED_KEY = "k8s-gitsync"


class HelmClient():
    def __init__(self, helm_binary_path="helm"):
        self.helm_binary_path = helm_binary_path

    def get_values(self, release_name):
        cmd = [self.helm_binary_path, "get", "values", release_name, "--output", "json"]
        outs, _, _ = utils.cmd_exec(cmd)
        values = json.loads(outs.decode())
        return values

    def get_release_list(self):
        cmd = [self.helm_binary_path, "list", "--output", "json"]
        outs, _, _ = utils.cmd_exec(cmd)

        # if no release exists, helm return empty binary b''
        if not outs:
            return []

        release_list = json.loads(outs.decode())
        return release_list["Releases"]

    def upgrade_install_release(self, release_name, namespace, version, chart_name, values):
        cmd = [self.helm_binary_path, "upgrade",
               "--output", "json",
               "--install", release_name,
               "--namespace", namespace,
               "--values", "-",
               "--version", version,
               chart_name]
        outs, _, _ = utils.cmd_exec(cmd, values)

        # remove WARNING:, DEBUG: Release
        warning_log_re = re.compile(r'^(WARNING:|DEBUG:|Release).*', re.MULTILINE)
        outs_json = warning_log_re.sub("", outs.decode())

        return json.loads(outs_json)

    def delete_release(self, release_name):
        cmd = [self.helm_binary_path, "delete", "--purge", release_name]
        outs, _, _ = utils.cmd_exec(cmd)
        return


def _calc_helm_values_hash(values_dict):
    # deep clone and remove managed key
    values = json.loads(json.dumps(values_dict))
    values.pop(KGS_MANAGED_KEY, None)
    json_str = json.dumps(values, sort_keys=True)
    return hashlib.sha256(json_str.encode()).hexdigest()


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


def _get_state(helm_client):
    release_list = helm_client.get_release_list()
    state = {}
    for e in release_list:
        state_id_str = f'helm.{e["Namespace"]}.{e["Name"]}'
        values = helm_client.get_values(e["Name"])
        state[state_id_str] = {
            "release_name": e["Name"],
            "chart": e["Chart"],
            "_values_data": values,
            "values_hash": _calc_helm_values_hash(values)}
    return state


def _get_manifest(helm_manifest_files):
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


def _check_create_or_upgrade(state_dict, manifest_dict):
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


def _check_delete(state_dict, manifest_dict):
    for id_str, state in state_dict.items():
        logger.debug(f'Checking helm releases ...')
        logger.debug(f'  {id_str}:')
        logger.debug(f"    managed       : {_safe_get(state, '_values_data', KGS_MANAGED_KEY, 'managed')}")
        logger.debug(f'    need to delete: {id_str not in manifest_dict}')

        if (_safe_get(state, '_values_data', KGS_MANAGED_KEY, 'managed') is True and
                id_str not in manifest_dict):
            yield id_str, state['release_name']


def create_or_update(helm_manifest_files):
    helm_client = HelmClient()
    state_dict = _get_state(helm_client)
    manifest_dict = _get_manifest(helm_manifest_files)

    for id_str, manifest, values in _check_create_or_upgrade(state_dict, manifest_dict):
        values[KGS_MANAGED_KEY] = {"managed": True}
        helm_client.upgrade_install_release(
            manifest['name'],
            manifest['namespace'],
            manifest['chart']['version'],
            manifest['chart']['name'],
            yaml.safe_dump(values).encode())


def destroy_unless_exist_in(helm_manifest_files):
    helm_client = HelmClient()
    state_dict = _get_state(helm_client)
    manifest_dict = _get_manifest(helm_manifest_files)

    for id_str, release_name in _check_delete(state_dict, manifest_dict):
        helm_client.delete_release(release_name)
