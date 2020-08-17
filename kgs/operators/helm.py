import json
import re

import yaml

from kgs import utils
from kgs.consts import KGS_MANAGED_KEY
from kgs.manifests.helm import HelmManifest
from kgs.result import Result
from kgs.result import ResultKind
from kgs.states.helm import HelmState


class HelmOperator:
    def __init__(self, helm_binary_path="helm", kubectl_binary_path="kubectl"):
        self.helm_binary_path = helm_binary_path
        self.kubectl_binary_path = kubectl_binary_path

    def get_release_list(self) -> dict:
        cmd = [self.helm_binary_path, "list", "--output", "json", "--all-namespaces"]
        outs, _, _ = utils.cmd_exec(cmd)
        return json.loads(outs.decode())

    def get_release(self, namespace: str, name: str) -> Result[dict]:
        release_list = self.get_release_list()
        for e in release_list:
            if e["namespace"] == namespace and e["name"] == name:
                return Result.ok(e)
        return Result.err({}, ResultKind.notfound)

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
        namespace = manifest.namespace
        name = manifest.name

        release, ret, [is_err, notfound] = self.get_release(namespace, name).chk(ResultKind.notfound)
        if is_err or notfound:
            return Result.chain(ret)

        values, ret, [is_err] = self.get_values(namespace, name).chk()
        if is_err:
            return Result.chain(ret)

        state = {
            "name": release["name"],
            "chart_id": release["chart"],
            "namespace": release["namespace"],
            "values": values,
        }
        return Result.ok(HelmState(**state))

    def _ensure_namespace(self, namespace):
        cmd = [self.kubectl_binary_path, "create", "namespace", namespace]
        return utils.cmd_exec(cmd)

    def create_or_update(self, manifest: HelmManifest, dry_run: bool) -> Result[dict]:
        state, result, [is_err, notfound] = self.get_state(manifest).chk(ResultKind.notfound)
        if is_err:
            return Result.chain(result)

        if not notfound and state.is_updated(manifest):
            return Result.ok({})

        if dry_run:
            return Result.ok({})

        cmd = []
        cmd += [self.helm_binary_path, "upgrade"]
        cmd += ["--install", manifest.name]
        cmd += ["--output", "json"]
        cmd += ["--create-namespace"]
        cmd += ["--namespace", manifest.namespace]
        cmd += ["--values", "-"]
        cmd += ["--version", manifest.chart.version]
        if manifest.chart.repo is not None and manifest.chart.repo != "":
            cmd += ["--repo", manifest.chart.repo]
        if manifest.chart.localpath is not None and manifest.chart.localpath != "":
            cmd += [f"{manifest.chart.localpath}{manifest.chart.name}"]
        else:
            cmd += [manifest.chart.name]

        values = {KGS_MANAGED_KEY: {"managed": True}}
        values.update(manifest.values)
        outs, _, rc = utils.cmd_exec(cmd, yaml.safe_dump(values).encode())
        if rc != 0:
            return Result.err({})

        # remove WARNING:, DEBUG: Release
        warning_log_re = re.compile(r"^(WARNING:|DEBUG:|Release).*", re.MULTILINE)
        _ = warning_log_re.sub("", outs.decode())

        # return json.loads(outs_json)
        return Result.ok({})
