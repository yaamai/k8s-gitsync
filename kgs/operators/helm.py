import json

from kgs import utils
from kgs.manifests.helm import HelmManifest
from kgs.result import Result
from kgs.result import ResultKind
from kgs.states.helm import HelmState


class HelmOperator:
    def __init__(self, helm_binary_path="helm", kubectl_binary_path="kubectl"):
        self.helm_binary_path = helm_binary_path
        self.kubectl_binary_path = kubectl_binary_path

    def get_release_list(self):
        cmd = [self.helm_binary_path, "list", "--output", "json", "--all-namespaces"]
        outs, _, _ = utils.cmd_exec(cmd)
        return json.loads(outs.decode())

    def get_values(self, namespace: str, release_name: str) -> Result[dict]:
        cmd = [
            self.helm_binary_path,
            "-n",
            namespace,
            "get",
            "values",
            release_name,
            "--output",
            "json",
        ]
        outs, errs, rc = utils.cmd_exec(cmd)
        values = json.loads(outs.decode())

        if ("release: not found" in errs.decode()) and rc != 0:
            return Result.err({"msg": "notfound"}, ResultKind.notfound)
        if values is None:
            return Result.err({"msg": "unknown"})
        if rc != 0:
            return Result.err({"msg": "unexpected return code", "raw": errs.decode()})

        return Result.ok(values)

    def get_state(self, manifest: HelmManifest) -> Result[HelmState]:
        breakpoint()
        namespace = manifest.get_namespace()
        name = manifest.get_name()
        chart = manifest.get_chart()

        values, ret, [is_err] = self.get_values(namespace, name).chk()
        if is_err:
            return Result.chain(ret)

        state = {
            "release_name": manifest.get_name(),
            "chart": chart,
            "namespace": namespace,
            "_values_data": values,
        }
        return Result.ok(HelmState(m=manifest, state=state))
