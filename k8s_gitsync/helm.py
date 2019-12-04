import re
import yaml
import json
import hashlib
import os.path
from . import utils
from . import log

logger = log.getLogger(__name__)

KGS_MANAGED_KEY = "k8s-gitsync"


class HelmV2Client():
    def __init__(self, helm_binary_path="./helm3/helm"):
        self.helm_binary_path = helm_binary_path

    def get_values(self, namespace, release_name):
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

        def _rename_key(helm_release):
            for from_key, to_key in [("Name", "name"), ("Chart", "chart"), ("Namespace", "namespace")]:
                if from_key in helm_release:
                    helm_release[to_key] = helm_release[from_key]
            return helm_release

        return [_rename_key(e) for e in release_list["Releases"]]

    def upgrade_install_release(self, namespace, release_name, repo, chart_name, version, values):
        cmd = []
        cmd += [self.helm_binary_path, "upgrade"]
        cmd += ["--output", "json"]
        cmd += ["--install", release_name]
        cmd += ["--namespace", namespace]
        cmd += ["--values", "-"]
        cmd += ["--version", version]
        if repo is not None and repo != "":
            cmd += ["--repo", repo]
        cmd += [chart_name]
        outs, _, _ = utils.cmd_exec(cmd, values)

        # remove WARNING:, DEBUG: Release
        warning_log_re = re.compile(r'^(WARNING:|DEBUG:|Release).*', re.MULTILINE)
        outs_json = warning_log_re.sub("", outs.decode())

        return json.loads(outs_json)

    def delete_release(self, namespace, release_name):
        cmd = [self.helm_binary_path, "delete", "--purge", release_name]
        outs, _, _ = utils.cmd_exec(cmd)
        return


class HelmV3Client(HelmV2Client):
    def __init__(self, helm_binary_path):
        super().__init__(helm_binary_path)
        self.helm_binary_path = helm_binary_path

    def _ensure_namespace(self, namespace):
        cmd = ["kubectl", "create", "namespace", namespace]
        utils.cmd_exec(cmd)

    def get_release_list(self):
        cmd = [self.helm_binary_path, "list", "--output", "json", "--all-namespaces"]
        outs, _, _ = utils.cmd_exec(cmd)
        return json.loads(outs.decode())

    def get_values(self, namespace, release_name):
        cmd = [self.helm_binary_path, "-n", namespace, "get", "values", release_name, "--output", "json"]
        outs, _, _ = utils.cmd_exec(cmd)
        values = json.loads(outs.decode())
        if values is None:
            return {}
        return values

    def _install_release(self, namespace, release_name, repo, chart_name, version, values):
        self._ensure_namespace(namespace)
        cmd = []
        cmd += [self.helm_binary_path, "install"]
        cmd += [release_name]
        cmd += [chart_name]
        cmd += ["--output", "json"]
        cmd += ["--namespace", namespace]
        cmd += ["--version", version]
        cmd += ["--values", "-"]
        if repo is not None and repo != "":
            cmd += ["--repo", repo]

        outs, _, _ = utils.cmd_exec(cmd, values)

        # remove WARNING:, DEBUG: Release
        warning_log_re = re.compile(r'^(WARNING:|DEBUG:|Release).*', re.MULTILINE)
        outs_json = warning_log_re.sub("", outs.decode())

        return json.loads(outs_json)

    def _upgrade_release(self, namespace, release_name, repo, chart_name, version, values):
        self._ensure_namespace(namespace)
        cmd = []
        cmd += [self.helm_binary_path, "upgrade"]
        cmd += ["--output", "json"]
        cmd += ["--install", release_name]
        cmd += ["--namespace", namespace]
        cmd += ["--values", "-"]
        cmd += ["--version", version]
        if repo is not None and repo != "":
            cmd += ["--repo", repo]
        cmd += [chart_name]
        outs, _, _ = utils.cmd_exec(cmd, values)

        # remove WARNING:, DEBUG: Release
        warning_log_re = re.compile(r'^(WARNING:|DEBUG:|Release).*', re.MULTILINE)
        outs_json = warning_log_re.sub("", outs.decode())

        return json.loads(outs_json)

    def _is_installed(self, release_name, namespace):
        release_list = self.get_release_list()
        for e in release_list:
            if e["namespace"] == namespace and e["name"] == release_name:
                return True
        return False

    def upgrade_install_release(self, namespace, release_name, repo, chart_name, version, values):
        # check installed or not
        # because values from stdin not fully supported yet on v3.0.0 (#7002)
        if self._is_installed(release_name, namespace):
            return self._upgrade_release(namespace, release_name, repo, chart_name, version, values)
        else:
            return self._install_release(namespace, release_name, repo, chart_name, version, values)

    def delete_release(self, namespace, release_name):
        cmd = [self.helm_binary_path, "delete", "-n", namespace, release_name]
        outs, _, _ = utils.cmd_exec(cmd)
        return


class HelmClient():
    def __init__(self, helm_binary_path="helm"):
        self.helm_binary_path = helm_binary_path

        v, _, _ = self._get_helm_version()
        if v == 2:
            self.client = HelmV2Client(self.helm_binary_path)
        if v == 3:
            self.client = HelmV3Client(self.helm_binary_path)

    def _get_helm_version(self):
        version_re = re.compile(r'.*v([0-9]+)\.([0-9]+)\.([0-9]+).*')
        cmd = [self.helm_binary_path, "version", "-c", "--short"]
        outs, _, _ = utils.cmd_exec(cmd)
        m = version_re.match(outs.decode())
        return int(m.group(1)), int(m.group(2)), int(m.group(3))

    def get_values(self, namespace, release_name):
        return self.client.get_values(namespace, release_name)

    def get_release_list(self):
        return self.client.get_release_list()

    def upgrade_install_release(self, namespace, release_name, repo, chart_name, version, values):
        return self.client.upgrade_install_release(namespace, release_name, repo, chart_name, version, values)

    def delete_release(self, namespace, release_name):
        return self.client.delete_release(namespace, release_name)


def _calc_helm_values_hash(values_dict):
    # deep clone and remove managed key
    values = json.loads(json.dumps(values_dict))
    values.pop(KGS_MANAGED_KEY, None)
    json_str = json.dumps(values, sort_keys=True)
    return hashlib.sha256(json_str.encode()).hexdigest()


def _safe_get(d, *args, default=None):
    r = d
    for k in args:
        if k not in r:
            return default
        r = r[k]
    return r


def _hash_head(s):
    if s is None:
        return s

    return s[:8]


def _get_state(helm_client):
    release_list = helm_client.get_release_list()
    state = {}
    for e in release_list:
        state_id_str = f'helm.{e["namespace"]}.{e["name"]}'
        values = helm_client.get_values(e["namespace"], e["name"])
        state[state_id_str] = {
            "release_name": e["name"],
            "chart": e["chart"],
            "namespace": e["namespace"],
            "_values_data": values,
            "values_hash": _calc_helm_values_hash(values)}
    return state


def _get_values(directory, value_files):
    # TODO: currently, only support one values file
    values = {}
    if len(value_files) > 0:
        values_file_path = os.path.join(directory, value_files[0])
        values = yaml.safe_load(open(values_file_path))
    return values


def _get_manifest(helm_manifest_files):
    manifest_dict = {}
    for directory, manifest_file_list in helm_manifest_files.items():
        for manifest_file in manifest_file_list:
            manifest_file_path = os.path.join(directory, manifest_file["manifest"])
            manifest = yaml.safe_load(open(manifest_file_path))
            values = _get_values(directory, manifest_file["values"])

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
        is_not_installed = id_str not in state_dict
        is_chart_mismatch = manifest['chart'] != _safe_get(state_dict, id_str, 'chart')
        is_values_mismatch = manifest['values_hash'] != _safe_get(state_dict, id_str, 'values_hash')
        need_process = (is_not_installed or is_chart_mismatch or is_values_mismatch)

        logger.debug(f'Checking helm releases ...')
        logger.info(f'  {id_str}: need install or upgrade {need_process}')
        logger.debug(f'    not installed: {id_str not in state_dict}')
        logger.debug(f'    chart ver    : {manifest["chart"]} <-> {_safe_get(state_dict, id_str, "chart")}')
        logger.debug(f'    values hash  : {_hash_head(manifest["values_hash"])}'
                     f' <-> {_hash_head(_safe_get(state_dict, id_str, "values_hash"))}')
        if need_process:
            yield id_str, manifest['_manifest_data'], manifest['_values_data']


def _check_delete(state_dict, manifest_dict):
    for id_str, state in state_dict.items():
        is_managed_resource = _safe_get(state, '_values_data', KGS_MANAGED_KEY, 'managed') is True
        is_deleted_in_manifest = id_str not in manifest_dict
        need_process = (is_managed_resource and is_deleted_in_manifest)

        logger.debug(f'Checking helm releases ...')
        logger.info(f'  {id_str}: need delete {need_process}')
        logger.debug(f"    managed       : {_safe_get(state, '_values_data', KGS_MANAGED_KEY, 'managed')}")
        logger.debug(f'    need to delete: {id_str not in manifest_dict}')

        if need_process:
            yield id_str, state['namespace'], state['release_name']


def create_or_update(helm_manifest_files):
    helm_client = HelmClient()
    state_dict = _get_state(helm_client)
    manifest_dict = _get_manifest(helm_manifest_files)

    for id_str, manifest, values in _check_create_or_upgrade(state_dict, manifest_dict):
        values[KGS_MANAGED_KEY] = {"managed": True}
        helm_client.upgrade_install_release(
            manifest['namespace'],
            manifest['name'],
            manifest['chart'].get('repo', None),
            manifest['chart']['name'],
            manifest['chart']['version'],
            yaml.safe_dump(values).encode())


def destroy_unless_exist_in(helm_manifest_files):
    helm_client = HelmClient()
    state_dict = _get_state(helm_client)
    manifest_dict = _get_manifest(helm_manifest_files)

    for id_str, namespace, release_name in _check_delete(state_dict, manifest_dict):
        helm_client.delete_release(namespace, release_name)
