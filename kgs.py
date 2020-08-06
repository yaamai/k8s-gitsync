from dataclasses import dataclass, field
import hashlib
from typing import List, Type
from typing import Final
from typing import Optional
from subprocess import Popen, PIPE
import json
import yaml
from dataclasses_json import dataclass_json


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


@dataclass_json
@dataclass
class K8SManifest():
    data: dict = field(default_factory=dict)  # , repr=False)

    def get_id(self):
        namespace = self.data.get("namespace", KGS_DEFAULT_NS)
        return f'{self.data["kind"].lower()}.{namespace}.{self.data["metadata"]["name"]}'

    def get_namespace(self):
        return _safe_get(self.data, "metadata", "namespace", default=KGS_DEFAULT_NS)

    def get_name(self):
        return _safe_get(self.data, "metadata", "name")

    def get_kind(self):
        return _safe_get(self.data, "kind")

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


@dataclass_json
@dataclass
class K8SState():
    m: K8SManifest
    state: dict = field(default_factory=dict)

    def is_updated(self) -> bool:
        current = _safe_get(self.state, "metadata", "annotations", LAST_APPLIED_KEY)
        expect = _safe_get(self.m.data, "metadata", "annotations", LAST_APPLIED_KEY)
        return current == expect


class K8SOperator():
    @staticmethod
    def _get_state(manifest) -> Optional[dict]:
        namespace = manifest.get_namespace()
        name = manifest.get_name()
        kind = manifest.get_kind()
        if not all([namespace, name, kind]):
            return None

        cmd = ["kubectl", "-n", namespace, "get", kind, name, "-o", "json"]
        outs, _, rc = cmd_exec(cmd)
        if rc != 0:
            return None

        return json.loads(outs.decode())

    def get_state(self, manifest: K8SManifest) -> Optional[K8SState]:
        state = self._get_state(manifest)
        if not state:
            return None
        return K8SState(m=manifest, state=state)

    @staticmethod
    def _ensure_namespace(namespace):
        cmd = ["kubectl", "create", "namespace", namespace]
        cmd_exec(cmd)

    def create_or_update(self, manifest: K8SManifest, dry_run: bool):
        state = self.get_state(manifest)
        if state and state.is_updated():
            return

        if dry_run:
            return

        self._ensure_namespace(manifest.get_namespace())

        cmd = ["kubectl", "apply", "-f", "-"]
        _, _, _ = cmd_exec(cmd, stdin=yaml.dump(manifest.data).encode())
