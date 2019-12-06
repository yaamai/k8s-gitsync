import json
import yaml
import hashlib
from collections import defaultdict
from . import log
from . import utils

logger = log.getLogger(__name__)

LAST_APPLIED_KEY = "k8s-gitsync/last-applied-confighash"
KGS_MANAGED_KEY = "k8s-gitsync/managed"


def _ensure_namespace(namespace):
    cmd = ["kubectl", "create", "namespace", namespace]
    utils.cmd_exec(cmd)


def _parse_manifest_file(filepath):
    with open(filepath) as f:
        filecontent = f.read()

    filehash = hashlib.sha256(filecontent.encode()).hexdigest()
    manifest = yaml.safe_load(filecontent)

    return (manifest, filehash)


def _get_state(manifest):
    namespace = manifest["metadata"]["namespace"]
    name = manifest["metadata"]["name"]
    kind = manifest["kind"]

    cmd = ["kubectl", "-n", namespace, "get", kind, name, "-o", "json"]
    outs, _, rc = utils.cmd_exec(cmd)
    if rc != 0:
        return None
    else:
        return json.loads(outs.decode())


def _apply_manifest(manifest, filehash):
    resource_id = _k8s_resource_id(manifest["kind"], manifest["metadata"])
    logger.info(f"applying {resource_id}")

    if manifest["metadata"].get("annotations") is None:
        manifest["metadata"]["annotations"] = {}
    manifest["metadata"]["annotations"][LAST_APPLIED_KEY] = filehash

    if manifest["metadata"].get("labels") is None:
        manifest["metadata"]["labels"] = {}
    manifest["metadata"]["labels"][KGS_MANAGED_KEY] = "true"

    _ensure_namespace(manifest["metadata"]["namespace"])

    cmd = ["kubectl", "apply", "-f", "-"]
    _, errs, _ = utils.cmd_exec(cmd, stdin=yaml.dump(manifest).encode())
    if errs:
        logger.error(f"failed to execute kubectl apply, {errs}")
    else:
        logger.info(f"applied {resource_id}")


def create_or_update(filepath):
    manifest, filehash = _parse_manifest_file(filepath)
    state = _get_state(manifest)

    if state is not None and filehash == state["metadata"].get("annotations", {}).get(LAST_APPLIED_KEY):
        return

    _apply_manifest(manifest, filehash)


def _k8s_resource_id(kind, metadata):
    return f'{kind.lower()}.{metadata["namespace"]}.{metadata["name"]}'


def destroy_unless_exist_in(manifest_filepaths):
    manifest_ids = []
    for filepath in manifest_filepaths:
        manifest, _ = _parse_manifest_file(filepath)
        manifest_ids.append(_k8s_resource_id(manifest["kind"], manifest["metadata"]))
    logger.info(f"existing manifests: {manifest_ids}")

    logger.info("fetching resouce kinds from k8s..")
    cmd = ["kubectl", "api-resources", "-o", "name"]
    outs, _, _ = utils.cmd_exec(cmd)
    kinds = outs.decode().split()
    kinds_csv = ",".join(kinds)
    logger.info("fetched resouce kinds from k8s.")

    logger.info("fetching all resources from k8s..")
    cmd = ["kubectl", "get", kinds_csv, "--all-namespaces", "-l", KGS_MANAGED_KEY + "=true", "-o", "json"]
    # NOTE: ignore stderr because it contains the messages that is output even when command does not fail.
    outs, _, _ = utils.cmd_exec(cmd)
    logger.info("fetched all resources from k8s.")

    states = json.loads(outs.decode())["items"]
    states = _filter_states_by_label(states, KGS_MANAGED_KEY, "true")
    logger.info(f'existing states: {[_k8s_resource_id(s["kind"], s["metadata"]) for s in states]}')

    for state in states:
        state_id = _k8s_resource_id(state["kind"], state["metadata"])
        if state_id not in manifest_ids:
            logger.info(f"{state_id} does not exist, it will be destroyed")
            _delete_state(state)
        else:
            logger.info(f"{state_id} exists")


def _filter_states_by_label(states, labelkey, labelvalue):
    # NOTE:
    # 'kubectl api-resources' may return some kinds that does not work for Selector (e.g. 'componentstatuses'),
    # so k8s resources must be checked with the label.
    def _(state):
        labels = state["metadata"].get("labels", {})
        logger.debug(f"labels: {labels}")
        value = labels.get(labelkey, None)
        if value is None:
            return False
        logger.debug(f"found; {labelkey}: {value}")
        if value != labelvalue:
            return False
        return True

    return list(filter(_, states))


def _delete_state(state):
    resource_id = _k8s_resource_id(state["kind"], state["metadata"])
    logger.info(f"deleting {resource_id}")
    cmd = ["kubectl", "-n", state["metadata"]["namespace"], "delete", state["kind"], state["metadata"]["name"]]
    _, errs, _ = utils.cmd_exec(cmd)
    if errs:
        logger.error(f"failed to delete {resource_id}: {errs.decode()}")
    logger.info(f"deleted {resource_id}")
