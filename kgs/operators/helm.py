import json
import re

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

        if ("release: not found" in errs.decode()) and rc != 0:
            return Result.err({"msg": "notfound"}, ResultKind.notfound)
        if rc != 0:
            return Result.err({"msg": "unexpected return code", "raw": errs.decode()})

        values = json.loads(outs.decode())
        return Result.ok(values)

    def get_state(self, manifest: HelmManifest) -> Result[HelmState]:
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

    def _ensure_namespace(self, namespace):
        cmd = [self.kubectl_binary_path, "create", "namespace", namespace]
        return utils.cmd_exec(cmd)

    def _is_installed(self, release_name, namespace):
        release_list = self.get_release_list()
        for e in release_list:
            if e["namespace"] == namespace and e["name"] == release_name:
                return True
        return False

    def create_or_update(self, manifest: HelmManifest, dry_run: bool):
        state, _, [is_err, notfound] = self.get_state(manifest).chk(ResultKind.notfound)
        if is_err:
            return

        if not notfound and state.is_updated():
            return

        if dry_run:
            return

        cmd = []
        cmd += [self.helm_binary_path, "upgrade"]
        cmd += ["--install", manifest.get_name()]
        cmd += ["--output", "json"]
        cmd += ["--create-namespace"]
        cmd += ["--namespace", manifest.get_namespace()]
        cmd += ["--values", "-"]
        cmd += ["--version", manifest.get_chart()["version"]]
        if manifest.get_chart()["repo"] is not None and manifest.get_chart()["repo"] != "":
            cmd += ["--repo", manifest.get_chart()["repo"]]
        if manifest.get_chart().get("localpath") is not None and manifest.get_chart()["localpath"] != "":
            cmd += [f"{manifest.get_chart()['localpath']}{manifest.get_chart()['name']}"]
        else:
            cmd += [manifest.get_chart()["name"]]
        outs, _, rc = utils.cmd_exec(cmd, manifest.get_values())
        if rc != 0:
            return
        breakpoint()

        # remove WARNING:, DEBUG: Release
        warning_log_re = re.compile(r"^(WARNING:|DEBUG:|Release).*", re.MULTILINE)
        _ = warning_log_re.sub("", outs.decode())

        # return json.loads(outs_json)
        return
