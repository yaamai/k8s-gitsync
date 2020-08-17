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
        namespace = manifest.metadata.namespace
        name = manifest.metadata.name
        kind = manifest.kind
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

    def create_or_update(self, manifest: K8SManifest, dry_run: bool) -> Result[dict]:
        state, result, [is_err, notfound] = self.get_state(manifest).chk(ResultKind.notfound)
        if is_err:
            return Result.chain(result)

        if not notfound and state.is_updated():
            return Result.ok({})

        if dry_run:
            return Result.ok({})

        self._ensure_namespace(manifest.metadata.namespace)

        cmd = ["kubectl", "apply", "-f", "-"]
        _, _, _ = utils.cmd_exec(cmd, stdin=yaml.dump(manifest.to_dict()).encode())

        return Result.ok({})