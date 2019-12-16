import re
import yaml
import json
import hashlib
from . import utils
from . import log

logger = log.getLogger(__name__)

KGS_MANAGED_KEY = "k8s-gitsync"


class HelmV2Client:
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

    def upgrade_install_release(self, namespace, release_name, repo, localpath, chart_name, version, values):
        cmd = []
        cmd += [self.helm_binary_path, "upgrade"]
        cmd += ["--output", "json"]
        cmd += ["--install", release_name]
        cmd += ["--namespace", namespace]
        cmd += ["--values", "-"]
        cmd += ["--version", version]
        if repo is not None and repo != "":
            cmd += ["--repo", repo]
        if localpath is not None and localpath != "":
            cmd += [f"{localpath}{chart_name}"]
        else:
            cmd += [chart_name]
        outs, _, _ = utils.cmd_exec(cmd, values)

        # remove WARNING:, DEBUG: Release
        warning_log_re = re.compile(r"^(WARNING:|DEBUG:|Release).*", re.MULTILINE)
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

    def _install_release(self, namespace, release_name, repo, localpath, chart_name, version, values):
        self._ensure_namespace(namespace)
        cmd = []
        cmd += [self.helm_binary_path, "install"]
        cmd += [release_name]
        if localpath is not None and localpath != "":
            cmd += [f"{localpath}{chart_name}"]
        else:
            cmd += [chart_name]
        cmd += ["--output", "json"]
        cmd += ["--namespace", namespace]
        cmd += ["--version", version]
        cmd += ["--values", "-"]
        if repo is not None and repo != "":
            cmd += ["--repo", repo]

        outs, _, _ = utils.cmd_exec(cmd, values)

        # remove WARNING:, DEBUG: Release
        warning_log_re = re.compile(r"^(WARNING:|DEBUG:|Release).*", re.MULTILINE)
        outs_json = warning_log_re.sub("", outs.decode())

        return json.loads(outs_json)

    def _upgrade_release(self, namespace, release_name, repo, localpath, chart_name, version, values):
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
        if localpath is not None and localpath != "":
            cmd += [f"{localpath}{chart_name}"]
        else:
            cmd += [chart_name]
        outs, _, _ = utils.cmd_exec(cmd, values)

        # remove WARNING:, DEBUG: Release
        warning_log_re = re.compile(r"^(WARNING:|DEBUG:|Release).*", re.MULTILINE)
        outs_json = warning_log_re.sub("", outs.decode())

        return json.loads(outs_json)

    def _is_installed(self, release_name, namespace):
        release_list = self.get_release_list()
        for e in release_list:
            if e["namespace"] == namespace and e["name"] == release_name:
                return True
        return False

    def upgrade_install_release(self, namespace, release_name, repo, localpath, chart_name, version, values):
        # check installed or not
        # because values from stdin not fully supported yet on v3.0.0 (#7002)
        if self._is_installed(release_name, namespace):
            return self._upgrade_release(namespace, release_name, repo, localpath, chart_name, version, values)
        else:
            return self._install_release(namespace, release_name, repo, localpath, chart_name, version, values)

    def delete_release(self, namespace, release_name):
        cmd = [self.helm_binary_path, "delete", "-n", namespace, release_name]
        outs, _, _ = utils.cmd_exec(cmd)
        return


class HelmClient:
    def __init__(self, helm_binary_path="helm"):
        self.helm_binary_path = helm_binary_path

        v, _, _ = self._get_helm_version()
        if v == 2:
            self.client = HelmV2Client(self.helm_binary_path)
        if v == 3:
            self.client = HelmV3Client(self.helm_binary_path)

    def _get_helm_version(self):
        version_re = re.compile(r".*v([0-9]+)\.([0-9]+)\.([0-9]+).*")
        cmd = [self.helm_binary_path, "version", "-c", "--short"]
        outs, _, _ = utils.cmd_exec(cmd)
        m = version_re.match(outs.decode())
        return int(m.group(1)), int(m.group(2)), int(m.group(3))

    def get_values(self, namespace, release_name):
        return self.client.get_values(namespace, release_name)

    def get_release_list(self):
        return self.client.get_release_list()

    def upgrade_install_release(self, namespace, release_name, repo, localpath, chart_name, version, values):
        return self.client.upgrade_install_release(
            namespace, release_name, repo, localpath, chart_name, version, values
        )

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
            "values_hash": _calc_helm_values_hash(values),
        }
    return state


def _get_values(value_files):
    # TODO: currently, only support one values file
    if len(value_files) != 1:
        return {}
    else:
        return yaml.safe_load(open(value_files[0]))


def _get_manifest(resource):
    manifest = yaml.safe_load(open(resource.manifest))
    values = _get_values(resource.values)

    id_str = f'helm.{manifest["namespace"]}.{manifest["name"]}'
    return {
        "id": id_str,
        "chart": f'{manifest["chart"]["name"]}-{manifest["chart"]["version"]}',
        "values_hash": _calc_helm_values_hash(values),
        "_manifest_data": manifest,
        "_values_data": values,
    }


def _check_create_or_upgrade(state_dict, manifest_dict):
    id_str = manifest_dict["id"]

    is_not_installed = id_str not in state_dict
    is_chart_mismatch = manifest_dict["chart"] != _safe_get(state_dict, id_str, "chart")
    is_values_mismatch = manifest_dict["values_hash"] != _safe_get(state_dict, id_str, "values_hash")
    need_process = is_not_installed or is_chart_mismatch or is_values_mismatch

    logger.info(f"Checking helm releases ...")
    logger.info(f"  {id_str}: need install or upgrade {need_process}")
    logger.debug(f"    not installed: {id_str not in state_dict}")
    logger.debug(f'    chart ver    : {manifest_dict["chart"]} <-> {_safe_get(state_dict, id_str, "chart")}')
    logger.debug(
        f'    values hash  : {_hash_head(manifest_dict["values_hash"])}'
        f' <-> {_hash_head(_safe_get(state_dict, id_str, "values_hash"))}'
    )
    return need_process


def _check_delete(state_dict, manifest_dict):
    for id_str, state in state_dict.items():
        is_managed_resource = _safe_get(state, "_values_data", KGS_MANAGED_KEY, "managed") is True
        is_deleted_in_manifest = id_str not in manifest_dict
        need_process = is_managed_resource and is_deleted_in_manifest

        logger.info(f"Checking helm releases ...")
        logger.info(f"  {id_str}: need delete {need_process}")
        logger.debug(f"    managed       : {_safe_get(state, '_values_data', KGS_MANAGED_KEY, 'managed')}")
        logger.debug(f"    need to delete: {id_str not in manifest_dict}")

        if need_process:
            yield id_str, state["namespace"], state["release_name"]


def create_or_update(resource, is_dry_run):
    helm_client = HelmClient()
    state_dict = _get_state(helm_client)
    manifest_dict = _get_manifest(resource)

    if _check_create_or_upgrade(state_dict, manifest_dict):
        manifest = manifest_dict["_manifest_data"]
        values = manifest_dict["_values_data"]

        values[KGS_MANAGED_KEY] = {"managed": True}
        if is_dry_run:
            logger.info("skipping install or upgrade a helm chart (dry-run)")
        else:
            helm_client.upgrade_install_release(
                manifest["namespace"],
                manifest["name"],
                manifest["chart"].get("repo", None),
                manifest["chart"].get("localpath", None),
                manifest["chart"]["name"],
                manifest["chart"]["version"],
                yaml.safe_dump(values).encode(),
            )


def destroy_unless_exist_in(resources, is_dry_run):
    helm_client = HelmClient()
    state_dict = _get_state(helm_client)
    manifests = [_get_manifest(r) for r in resources]
    manifest_dict = {m["id"]: m for m in manifests}

    for id_str, namespace, release_name in _check_delete(state_dict, manifest_dict):
        if is_dry_run:
            logger.info("skipping delete a helm chart (dry-run)")
        else:
            helm_client.delete_release(namespace, release_name)


def expand(resource):
    resource.content = _get_manifest(resource)
    resource.id = resource.content["id"]
    requires = resource.content["_manifest_data"].get("requires")
    if requires:
        if isinstance(requires, list):
            resource.requires = set(requires)
        elif isinstance(requires, str):
            resource.requires = set(requires.split(","))
        else:
            logger.warning("helm release's requires must be a list or a comma-separated string")
    else:
        resource.requires = set()
    return [resource]
