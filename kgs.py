import hashlib
import json
import re
from collections import OrderedDict
from dataclasses import dataclass
from dataclasses import field
from enum import Enum
from pathlib import Path
from subprocess import PIPE
from subprocess import Popen
from typing import Callable
from typing import Final
from typing import Generic
from typing import Iterable
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type
from typing import TypeVar

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


class Manifest(Protocol):
    def get_id(self) -> str:
        ...

    @classmethod
    def parse_dict(cls: Type["Manifest"], d: dict) -> "Manifest":
        ...


@dataclass_json
@dataclass
class HelmManifest(Manifest):
    data: dict = field(default_factory=dict)  # , repr=False)
    values: dict = field(default_factory=dict)  # , repr=False)

    def get_name(self) -> str:
        return _safe_get(self.data, "name")

    def get_namespace(self) -> str:
        return _safe_get(self.data, "namespace")

    def get_chart(self) -> str:
        return _safe_get(self.data, "chart")

    @classmethod
    def parse_dict(cls: Type["HelmManifest"], d: dict) -> "HelmManifest":
        return HelmManifest(data=d["manifest"], values=d["values"])

    @classmethod
    def parse_file(cls: Type["HelmManifest"], helm_file: str, values_files: List[str]) -> List["HelmManifest"]:
        d: dict = {"manifest": {}, "values": {}}

        with open(helm_file) as f:
            d["manifest"].update(yaml.safe_load(f))
        for values_file in values_files:
            with open(values_file) as f:
                d["values"].update(yaml.safe_load(f))

        return [cls.parse_dict(d)]


@dataclass_json
@dataclass
class K8SManifest(Manifest):
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
    def parse_dict(cls: Type["K8SManifest"], d: dict) -> "K8SManifest":
        return K8SManifest(data=cls._annotate_manifest_data(d))

    @classmethod
    def parse_array_of_dict(cls: Type["K8SManifest"], ary: List[dict]) -> List["K8SManifest"]:
        return [cls.parse_dict(elm) for elm in ary]

    @classmethod
    def parse_file(cls: Type["K8SManifest"], filepath: str) -> List["K8SManifest"]:
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
class K8SState:
    m: K8SManifest
    state: dict = field(default_factory=dict)

    def is_updated(self) -> bool:
        current = _safe_get(self.state, "metadata", "annotations", LAST_APPLIED_KEY)
        expect = _safe_get(self.m.data, "metadata", "annotations", LAST_APPLIED_KEY)
        return current == expect


T = TypeVar("T")


class ResultKind(Enum):
    unknown = "unknown"
    notfound = "notfound"


@dataclass
class Result(Generic[T]):
    result: Optional[T]
    detail: Optional[dict]
    is_err: bool
    kind: ResultKind = ResultKind.unknown

    @staticmethod
    def ok(result: T, detail: Optional[dict] = None) -> "Result[T]":
        return Result[T](result, detail, False)

    @staticmethod
    def err(detail: dict, kind: ResultKind = ResultKind.unknown) -> "Result[T]":
        return Result[T](None, detail, True, kind)

    @staticmethod
    def chain(result: "Result[T]"):
        return Result[T](None, result.detail, result.is_err, result.kind)

    # unwrap result and check err
    def chk(self, *args: ResultKind) -> Tuple[T, "Result[T]", List[bool]]:
        kind_chk_result = [self.kind == k for k in args]
        return self.result, self, [self.is_err and not any(kind_chk_result)] + kind_chk_result  # type:ignore


# T, None: on success
# T, {}: on success with additional info
# None, None: on failure with no additional info
# None, {}: on failure with additional info
# T, T: ?
# None, T: ?
# {}, None: ?
# {}, {}: ?
# {}, T: ?
class K8SOperator:
    @staticmethod
    def get_state(manifest: K8SManifest) -> Result[K8SState]:
        namespace = manifest.get_namespace()
        name = manifest.get_name()
        kind = manifest.get_kind()
        if not all([namespace, name, kind]):
            return Result.err({"msg": "invalid manifest"})

        cmd = ["kubectl", "-n", namespace, "get", kind, name, "-o", "json"]
        outs, errs, rc = cmd_exec(cmd)
        if ("(NotFound):" in errs.decode()) and rc != 0:
            return Result.err({"msg": "state not found"}, ResultKind.notfound)

        if rc != 0:
            return Result.err({"msg": "unexpected return code", "raw": errs.decode()})

        return Result.ok(K8SState(m=manifest, state=json.loads(outs.decode())))

    @staticmethod
    def _ensure_namespace(namespace):
        cmd = ["kubectl", "create", "namespace", namespace]
        return cmd_exec(cmd)

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
        _, _, _ = cmd_exec(cmd, stdin=yaml.dump(manifest.data).encode())

        return


@dataclass_json
@dataclass
class HelmState:
    m: HelmManifest
    state: dict = field(default_factory=dict)

    def _calc_helm_values_hash(self, values_dict):
        # deep clone and remove managed key
        values = json.loads(json.dumps(values_dict))
        values.pop(KGS_MANAGED_KEY, None)
        json_str = json.dumps(values, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()

    def is_updated(self) -> bool:
        current = _safe_get(self.state, "metadata", "annotations", LAST_APPLIED_KEY)
        expect = _safe_get(self.m.data, "metadata", "annotations", LAST_APPLIED_KEY)
        return current == expect


class HelmOperator:
    def __init__(self, helm_binary_path="helm", kubectl_binary_path="kubectl"):
        self.helm_binary_path = helm_binary_path
        self.kubectl_binary_path = kubectl_binary_path

    def get_release_list(self):
        cmd = [self.helm_binary_path, "list", "--output", "json", "--all-namespaces"]
        outs, _, _ = cmd_exec(cmd)
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
        outs, errs, rc = cmd_exec(cmd)
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


def _get_files_in_samedir(paths: Iterable[Path], filepath: Path, condition: Callable[[str], bool]) -> List[Path]:
    dir_files = [p for p in paths if list(p.parents) == list(filepath.parents)]
    return [p for p in dir_files if condition(str(p))]


def _get_files_in_samedir_pattern(paths: Iterable[Path], filepath: Path, pattern: re.Pattern, name: str) -> List[Path]:
    def _(path: str) -> bool:
        m = pattern.match(path)
        return m is not None and m.group(1) == name

    return _get_files_in_samedir(paths, filepath, _)


def _unwrap_any(*args: Optional[re.Match]) -> re.Match:
    for opt in args:
        if opt:
            return opt
    # this function expect least one of args must have value
    raise Exception()


HELM_MANIFEST_FILE_PATTERN: re.Pattern = re.compile(r"(.*)\.helm$")
HELM_MANIFEST_VALUES_FILE_PATTERN: re.Pattern = re.compile(r"(.*)\.values\.ya?ml$")


def _try_parse_helm(filepath: Path, paths: "OrderedDict[Path, Optional[bool]]") -> Optional[List[HelmManifest]]:
    match_helm = HELM_MANIFEST_FILE_PATTERN.match(str(filepath))
    match_values = HELM_MANIFEST_VALUES_FILE_PATTERN.match(str(filepath))
    if not match_helm and not match_values:
        return None

    filename = _unwrap_any(match_helm, match_values).group(1)
    values_files = _get_files_in_samedir_pattern(paths, filepath, HELM_MANIFEST_VALUES_FILE_PATTERN, filename)
    helm_file: Path = filepath
    if match_values:
        # rescan helm file based on values file's name
        helm_file_candidate = _get_files_in_samedir_pattern(paths, filepath, HELM_MANIFEST_FILE_PATTERN, filename)
        if len(helm_file_candidate) != 1:
            # when only values file found, skip related files
            for val in values_files:
                paths[val] = False
            return []
        helm_file = helm_file_candidate[0]

    paths[helm_file] = True
    for val in values_files:
        paths[val] = True
    return HelmManifest.parse_file(str(helm_file), [str(p) for p in values_files])


K8S_MANIFEST_FILE_PATTERN: re.Pattern = re.compile(r"(.*)\.ya?ml$")


def _try_parse_k8s(filepath: Path, paths: "OrderedDict[Path, Optional[bool]]") -> Optional[List[K8SManifest]]:
    m = K8S_MANIFEST_FILE_PATTERN.match(str(filepath))
    if not m:
        return None

    manifest = K8SManifest.parse_file(str(filepath))
    paths[filepath] = True
    return manifest


def load_recursively(repo_path: str) -> List[Manifest]:
    # path -> None(not processed), False(skipped), True(processed)
    paths: "OrderedDict[Path, Optional[bool]]" = OrderedDict()
    for path in Path(repo_path).glob("**/*"):
        paths[path] = None

    manifest_list: List[Manifest] = []
    for filepath, processed in paths.items():
        if processed is not None:
            continue

        result = _try_parse_helm(filepath, paths)
        if result is not None:
            manifest_list.extend(result)
            continue

        k8s_result = _try_parse_k8s(filepath, paths)
        if k8s_result is not None:
            manifest_list.extend(k8s_result)
            continue

    return manifest_list


if __name__ == "__main__":
    breakpoint()
    mf = HelmManifest.parse_file("test.helm", [])
    oper = HelmOperator()
    oper.create_or_update()

    # mf = K8SManifest.parse_file("test.yaml")
    # oper = K8SOperator()
    # oper.create_or_update(mf[0], dry_run=False)
