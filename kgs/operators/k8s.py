import json

import yaml

from kgs import utils
from kgs.manifests.k8s import K8SManifest
from kgs.result import Result
from kgs.result import ResultKind
from kgs.states.k8s import K8SState


class K8SOperator:
    @staticmethod
    def get_state(manifest: K8SManifest) -> Result[K8SState]:
        namespace = manifest.get_namespace()
        name = manifest.get_name()
        kind = manifest.get_kind()
        if not all([namespace, name, kind]):
            return Result.err({"msg": "invalid manifest"})

        cmd = ["kubectl", "-n", namespace, "get", kind, name, "-o", "json"]
        outs, errs, rc = utils.cmd_exec(cmd)
        if ("(NotFound):" in errs.decode()) and rc != 0:
            return Result.err({"msg": "state not found"}, ResultKind.notfound)

        if rc != 0:
            return Result.err({"msg": "unexpected return code", "raw": errs.decode()})

        return Result.ok(K8SState(m=manifest, state=json.loads(outs.decode())))

    @staticmethod
    def _ensure_namespace(namespace):
        cmd = ["kubectl", "create", "namespace", namespace]
        return utils.cmd_exec(cmd)

    def create_or_update(self, manifest: K8SManifest, dry_run: bool):
        state, _, [is_err, notfound] = self.get_state(manifest).chk(ResultKind.notfound)
        if is_err:
            return

        if not notfound and state.is_updated():
            return

        if dry_run:
            return

        self._ensure_namespace(manifest.get_namespace())

        cmd = ["kubectl", "apply", "-f", "-"]
        _, _, _ = utils.cmd_exec(cmd, stdin=yaml.dump(manifest.data).encode())

        return
