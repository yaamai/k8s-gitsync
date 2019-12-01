import json
import yaml
import hashlib
from collections import defaultdict
import log
import utils

logger = log.getLogger(__name__)

LAST_APPLIED_KEY = "k8s-gitsync/last-applied-confighash"
KGS_LABEL = "k8s-gitsync/mamaged"


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
    if manifest["metadata"].get("annotations") is None:
        manifest["metadata"]["annotations"] = {}
    manifest["metadata"]["annotations"][LAST_APPLIED_KEY] = filehash

    _ensure_namespace(manifest["metadata"]["namespace"])

    cmd = ["kubectl", "apply", "-f", "-"]
    _, errs, _ = utils.cmd_exec(cmd)
    if errs:
        logger.error(f"failed to execute kubectl apply, {errs}")


def create_or_update(filepath):
    manifest, filehash = _parse_manifest_file(filepath)
    state = _get_state(manifest)

    if state is not None and filehash == state["metadata"].get("annotations", {}).get(LAST_APPLIED_KEY):
        return

    logger.info(f"applying {manifest['metadata']}")
    _apply_manifest(manifest, filehash)
    logger.info(f"applied {manifest['metadata']}")


def destroy_unless_exist_in(manifest_filepaths):
    manifest_ids_per_kind = defaultdict(list)
    for filepath in manifest_filepaths:
        m, _ = _parse_manifest_file(filepath)
        manifest_id = f'{m["matadata"]["namespace"]}.{m["metadata"]["name"]}'
        manifest_ids_per_kind[m["kind"]].append(manifest_id)

    cmd = ["kubectl", "api-resources"]
    outs, _, _ = utils.cmd_exec(cmd)
    kinds = outs.decode().split()

    for kind in kinds:
        cmd = ["kubectl", "--all-namespaces", "-l", KGS_LABEL+"=true", "get", kind, "-o", "json"]
        outs, _, _ = utils.cmd_exec(cmd)
        states = json.loads(outs.decode())["items"]

        for s in states:
            state_id = f'{s["matadata"]["namespace"]}.{s["metadata"]["name"]}'
            if state_id not in manifest_ids_per_kind[kind]:
                _delete_state(state)


def _delete_state(state):
    pass
