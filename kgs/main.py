import argparse
import asyncio
import sys
from typing import List

from kgs import loader
from kgs import utils
from kgs.common import Manifest
from kgs.helm.manifest import HelmManifest
from kgs.helm.operator import HelmOperator
from kgs.k8s.manifest import K8SManifest
from kgs.k8s.operator import K8SOperator


logger = utils.get_logger(__name__)


def _load_manifests(repo: str) -> List[Manifest]:
    manifests, result, [is_err] = loader.load_recursively(repo).chk()
    if is_err:
        logger.error(result)
        return []

    if result.detail:
        t = ["    {:64.64}-> {}".format(str(p), str(k)) for (p, k) in result.detail["paths"].items()]
        logger.info("Parsing manifests:\n{}".format("\n".join(t)))

    return manifests


def async_upgrade_or_install(conf):
    return asyncio.run(_async_upgrade_or_install(conf))


async def _async_upgrade_or_install(conf):
    # probe k8s
    msg, ok = utils.probe_k8s()
    if not ok:
        print(msg)
        sys.exit(1)

    manifests = _load_manifests(conf.repo)
    manifest_map = {}
    for m in manifests:
        manifest_map[m.get_id()] = m

    sorter = loader.get_topo_sorter(conf.repo, manifests)

    # prepare operator
    operator_map = {}
    operator_map[K8SManifest] = K8SOperator()
    operator_map[HelmManifest] = HelmOperator()

    pending_tasks = []
    sorter.prepare()
    while sorter.is_active():
        for node in sorter.get_ready():
            manifest = manifest_map.get(node)
            oper = operator_map[manifest.__class__]
            task = asyncio.create_task(
                oper.create_or_update(manifest, dry_run=conf.dry_run, wait=True), name=manifest.get_id()
            )
            pending_tasks.append(task)

        done_set, pending_tasks = await asyncio.wait(pending_tasks, return_when=asyncio.FIRST_COMPLETED)
        for done in done_set:
            manifest = manifest_map.get(done.get_name())
            sorter.done(manifest.get_id())

            logger.info("\n    {:64.64}-> {}".format(manifest.get_id(), done.result()))


def upgrade_or_install(conf):
    # probe k8s
    msg, ok = utils.probe_k8s()
    if not ok:
        print(msg)
        sys.exit(1)

    manifests = _load_manifests(conf.repo)
    manifests = list(loader.sorted_manifests(conf.repo, manifests))
    logger.info("Loaded manifests:\n{}".format("\n".join(["    {}".format(str(m)) for m in manifests])))

    # prepare operator
    operator_map = {}
    operator_map[K8SManifest] = K8SOperator()
    operator_map[HelmManifest] = HelmOperator()

    for manifest in manifests:
        oper = operator_map[manifest.__class__]
        ret = oper.create_or_update(manifest, dry_run=conf.dry_run, wait=True)
        logger.info("\n    {:64.64}-> {}".format(manifest.get_id(), ret))


def list_id(conf):
    manifests = _load_manifests(conf.repo)
    for manifest in manifests:
        print(manifest.get_id())


def _parse_conf_and_action():
    cli_desc = "Synchronizing the states with the manifests of k8s/helm"
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument("repo", help="manifests repository directory")
    parser.add_argument("--clean", action="store_true", help="clean up the resources removed from manifests")
    parser.add_argument("--list-id", action="store_true", help="show resource id list")
    parser.add_argument("--bench-k8s-get", action="store_true", help="benchmark k8s get operation")
    parser.add_argument("--dry-run", action="store_true", help="dry run (check differences only)")
    conf = parser.parse_args()

    action = async_upgrade_or_install
    if conf.list_id:
        return conf, list_id

    return conf, action


def main():
    conf, action = _parse_conf_and_action()
    action(conf)


if __name__ == "__main__":
    main()
