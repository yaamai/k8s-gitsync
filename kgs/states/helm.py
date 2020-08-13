import hashlib
import json
from dataclasses import dataclass
from dataclasses import field

from dataclasses_json import dataclass_json

from kgs.consts import KGS_MANAGED_KEY
from kgs.consts import LAST_APPLIED_KEY
from kgs.manifests.helm import HelmManifest
from kgs.utils import _safe_get


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
