from dataclasses import dataclass
from dataclasses import field

from dataclasses_json import dataclass_json

from kgs.consts import LAST_APPLIED_KEY
from kgs.manifests.k8s import K8SManifest
from kgs.utils import _safe_get


@dataclass_json
@dataclass
class K8SState:
    m: K8SManifest
    state: dict = field(default_factory=dict)

    def is_updated(self) -> bool:
        current = _safe_get(self.state, "metadata", "annotations", LAST_APPLIED_KEY)
        expect = _safe_get(self.m.to_dict(), "metadata", "annotations", LAST_APPLIED_KEY)
        return current == expect
