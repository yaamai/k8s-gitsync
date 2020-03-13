import json
from typing import TypeVar
import yaml
from typing_extensions import Protocol
from . import utils
from .manifest import Manifest, K8SManifest
from .consts import KGS_DEFAULT_NS

T = TypeVar('T', bound='Manifest')


class Applier(Protocol):
    def create_or_update(self, manifest: Manifest):
        ...

    def destroy_unless_exist_in(self, manifest: Manifest):
        ...


class State(Protocol):
    def is_updated(self) -> bool:
        ...


class K8SState(State):
    m: K8SManifest

    def is_updated(self) -> bool:
        pass

    def from_manifest(self, manifest: K8SManifest) -> 'K8SState':
        pass

    @staticmethod
    def _ensure_namespace(namespace):
        cmd = ["kubectl", "create", "namespace", namespace]
        utils.cmd_exec(cmd)

    def _get_state(self, manifest):
        namespace = manifest["metadata"].get("namespace", KGS_DEFAULT_NS)
        name = manifest["metadata"]["name"]
        kind = manifest["kind"]

        cmd = ["kubectl", "-n", namespace, "get", kind, name, "-o", "json"]
        outs, _, rc = utils.cmd_exec(cmd)
        if rc != 0:
            return None

        return json.loads(outs.decode())

    def create_or_update(self, dry_run: bool):
        if self.is_updated():
            return

        if dry_run:
            return

        K8SState._ensure_namespace(self.m.data["metadata"].get("namespace", KGS_DEFAULT_NS))

        cmd = ["kubectl", "apply", "-f", "-"]
        _, _, _ = utils.cmd_exec(cmd, stdin=yaml.dump(self.m.data).encode())
