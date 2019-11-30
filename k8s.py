import json
import yaml
import hashlib
from subprocess import Popen, PIPE
import log

logger = log.getLogger(__name__)

LAST_APPLIED_KEY = "k8s-gitsync/last-applied-confighash"


def _ensure_namespace(namespace):
    cmd = ["kubectl", "create", "namespace", namespace]
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)
    outs, errs = p.communicate()
    log.command_result_debug(logger, cmd, outs, errs)


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
    p = Popen(cmd, stdout=PIPE, stderr=PIPE)
    outs, errs = p.communicate()
    log.command_result_debug(logger, cmd, outs, errs)
    if p.returncode != 0:
        return None
    else:
        return json.loads(outs.decode())


def _apply_manifest(manifest, filehash):
    if manifest["metadata"].get("annotations") is None:
        manifest["metadata"]["annotations"] = {}
    manifest["metadata"]["annotations"][LAST_APPLIED_KEY] = filehash

    _ensure_namespace(manifest["metadata"]["namespace"])

    cmd = ["kubectl", "apply", "-f", "-"]
    p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    outs, errs = p.communicate(yaml.dump(manifest).encode())
    log.command_result_debug(logger, cmd, outs, errs)
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
    pass
