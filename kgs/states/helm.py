import hashlib
import json
from dataclasses import dataclass

from dataclasses_json import dataclass_json

from kgs.consts import KGS_DEFAULT_NS
from kgs.consts import KGS_MANAGED_KEY
from kgs.manifests.helm import HelmManifest


@dataclass_json
@dataclass
class HelmState:
    name: str
    chart_id: str
    values: dict
    namespace: str = KGS_DEFAULT_NS

    @staticmethod
    def _calc_helm_values_hash(values_dict):
        # deep clone and remove managed key
        values = json.loads(json.dumps(values_dict))
        values.pop(KGS_MANAGED_KEY, None)
        json_str = json.dumps(values, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()

    def is_updated(self, manifest: HelmManifest) -> bool:
        is_chart_equals = f"{manifest.chart.name}-{manifest.chart.version}" == self.chart_id
        is_values_equals = HelmState._calc_helm_values_hash(manifest.values) == HelmState._calc_helm_values_hash(
            self.values
        )
        return all([is_chart_equals, is_values_equals])
