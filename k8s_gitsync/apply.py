import json
import yaml
import hashlib
from subprocess import Popen, PIPE


def ensure_namespace(namespace):
    cmd = ["kubectl", "create", "namespace", namespace]
    p = Popen(cmd, stdout=PIPE)
    return p.communicate()


def parse_manifest_file(filepath):
    with open(filepath) as f:
        filecontent = f.read()

    filehash = hashlib.sha256(filecontent.encode()).hexdigest()
    manifest = yaml.safe_load(filecontent)

    return (filehash, manifest)


def different_manifest_and_state(filepath):
    filehash, manifest = parse_manifest_file(filepath)

    namespace = manifest["metadata"]["namespace"]
    name = manifest["metadata"]["name"]
    kind = manifest["kind"]

    cmd = ["kubectl", "-n", namespace, "get", kind, name, "-o", "json"]
    p = Popen(cmd, stdout=PIPE)
    outs, errs = p.communicate()
    state = json.loads(outs.decode())

    statehash = state["metadata"]["last-applied-confighash"]

    return filehash != statehash


def apply(filepath):
    filehash, manifest = parse_manifest_file(filepath)
    manifest["metadata"]["last-applied-confighash"] = filehash

    cmd = ["kubectl", "-f", "-"]
    p = Popen(cmd, stdout=PIPE)
    return p.communicate(yaml.dump(manifest))
