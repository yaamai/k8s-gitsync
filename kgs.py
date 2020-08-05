from dataclasses import dataclass, field
import hashlib
from typing import List, Type
from typing import Final
from subprocess import Popen, PIPE
import json
import yaml
from dataclasses_json import dataclass_json
from typing_extensions import Protocol


KGS_DEFAULT_NS: Final[str] = "default"
LAST_APPLIED_KEY: Final[str] = "k8s-gitsync/last-applied-confighash"
KGS_MANAGED_KEY: Final[str] = "k8s-gitsync/managed"


def cmd_exec(cmd, stdin=None):
    proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE)
    outs, errs = proc.communicate(stdin)
    return outs, errs, proc.returncode


def _safe_get(d: dict, *args: str, default=None):
    r = d
    for k in args:
        if k not in r:
            return default
        r = r[k]
    return r


# manifest represents resource definitions identified by id
class Manifest(Protocol):
    def get_id(self) -> str:
        ...


@dataclass_json
@dataclass
class K8SManifest(Manifest):
    data: dict = field(default_factory=dict)  # , repr=False)

    def get_id(self):
        namespace = self.data.get("namespace", KGS_DEFAULT_NS)
        return f'{self.data["kind"].lower()}.{namespace}.{self.data["metadata"]["name"]}'

    @staticmethod
    def _annotate_manifest_data(data: dict) -> dict:
        hashhex = hashlib.sha256(yaml.dump(data).encode()).hexdigest()
        if "metadata" not in data:
            data["metadata"] = {}
        if "annotations" not in data["metadata"]:
            data["metadata"]["annotations"] = {}
        data["metadata"]["annotations"][LAST_APPLIED_KEY] = hashhex
        if "labels" not in data["metadata"]:
            data["metadata"]["labels"] = {}
        data["metadata"]["labels"][KGS_MANAGED_KEY] = "true"

        return data

    @classmethod
    def parse_dict(cls: Type['K8SManifest'], d: dict) -> 'K8SManifest':
        return K8SManifest(data=cls._annotate_manifest_data(d))

    @classmethod
    def parse_array_of_dict(cls: Type['K8SManifest'], ary: List[dict]) -> List['K8SManifest']:
        return [cls.parse_dict(elm) for elm in ary]

    @classmethod
    def parse_file(cls: Type['K8SManifest'], filepath: str) -> List['K8SManifest']:
        with open(filepath) as f:
            # some k8s manifest file has empty document
            documents = yaml.safe_load_all(f)
            manifests = []
            for document in documents:
                if document is None or not isinstance(document, dict):
                    continue
                manifests.append(cls.parse_dict(document))
        return manifests


class State(Protocol):
    def is_updated(self) -> bool:
        ...


class Operator(Protocol):
    def get_state(self, manifest: Manifest) -> State:
        ...

    def create_or_update(self, manifest: Manifest, dry_run: bool):
        ...


@dataclass_json
@dataclass
class K8SState(State):
    m: K8SManifest
    state: dict = field(default_factory=dict)

    def is_updated(self) -> bool:
        current = _safe_get(self.state, "metadata", "annotations", LAST_APPLIED_KEY)
        expect = _safe_get(self.m.data, "metadata", "annotations", LAST_APPLIED_KEY)
        return current == expect


class K8SOperator(Operator):
    # TODO: optional return
    def _get_state(self, manifest):
        namespace = manifest["metadata"].get("namespace", KGS_DEFAULT_NS)
        name = manifest["metadata"]["name"]
        kind = manifest["kind"]

        cmd = ["kubectl", "-n", namespace, "get", kind, name, "-o", "json"]
        outs, _, rc = cmd_exec(cmd)
        if rc != 0:
            return None

        return json.loads(outs.decode())

    def get_state(self, manifest: Manifest) -> State:
        return K8SState(m=manifest, state=self._get_state(manifest))

    def create_or_update(self, manifest: Manifest, dry_run: bool):
        if self.get_state(manifest).is_updated():
            return

        if dry_run:
            return

        K8SState._ensure_namespace(self.m.data["metadata"].get("namespace", KGS_DEFAULT_NS))

        cmd = ["kubectl", "apply", "-f", "-"]
        _, _, _ = utils.cmd_exec(cmd, stdin=yaml.dump(self.m.data).encode())
