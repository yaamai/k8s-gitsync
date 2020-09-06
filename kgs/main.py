import argparse
import sys

from kgs import loader
from kgs import utils
from kgs.helm.manifest import HelmManifest
from kgs.helm.operator import HelmOperator
from kgs.k8s.manifest import K8SManifest
from kgs.k8s.operator import K8SOperator


logger = utils.get_logger(__name__)


def upgrade_or_install(conf):
    # probe k8s
    if not utils.probe_k8s():
        sys.exit(1)

    manifests, result, [is_err] = loader.load_recursively(conf.repo).chk()
    if is_err:
        logger.error("")

    t = ["    {:64.64}-> {}".format(str(p), str(k)) for (p, k) in result.detail["paths"].items()]
    logger.info("Parsing manifests:\n{}".format("\n".join(t)))
    logger.info("Loaded manifests:\n{}".format("\n".join(["    {}".format(str(m)) for m in manifests])))

    # prepare operator
    operator_map = {}
    operator_map[K8SManifest] = K8SOperator()
    operator_map[HelmManifest] = HelmOperator()

    for manifest in manifests:
        oper = operator_map[manifest.__class__]
        ret = oper.create_or_update(manifest, dry_run=conf.dry_run)
        logger.info("\n    {:64.64}-> {}".format(manifest.get_id(), ret))


def _parse_conf_and_action():
    cli_desc = "Synchronizing the states with the manifests of k8s/helm"
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument("repo", help="manifests repository directory")
    parser.add_argument("--clean", action="store_true", help="clean up the resources removed from manifests")
    parser.add_argument("--list-id", action="store_true", help="show resource id list")
    parser.add_argument("--bench-k8s-get", action="store_true", help="benchmark k8s get operation")
    parser.add_argument("--dry-run", action="store_true", help="dry run (check differences only)")
    conf = parser.parse_args()

    action = upgrade_or_install

    return conf, action


def main():
    conf, action = _parse_conf_and_action()
    action(conf)


if __name__ == "__main__":
    main()
