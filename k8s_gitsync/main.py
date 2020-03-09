import sys
import argparse
from toposort import toposort_flatten  # type: ignore
from . import utils
from . import k8s
from . import helm
from . import log

logger = log.get_logger(__name__)


def _load_resources(repo):
    # find all manifest files
    resources = utils.get_manifest_files(repo)

    # preload k8s manifests and expand multi-document YAML
    expanded_resources = []
    for resource in resources:
        if resource.applier == "k8s":
            expanded_resources.extend(k8s.expand_multi_document_file(resource))
        if resource.applier == "helm":
            expanded_resources.extend(helm.expand(resource))
    resources = expanded_resources
    return resources


def _arrange_resources(resources):
    # arrange by dependencies
    dep_graph = {r.id: r.requires for r in resources}
    dep_sorted = toposort_flatten(dep_graph)
    sorted_resources = []
    for resource_id in dep_sorted:
        resource = [r for r in resources if r.id == resource_id]
        if len(resource) == 1:
            sorted_resources.append(resource[0])
    resources = sorted_resources
    return resources


def upgrade_or_install(conf):
    # probe k8s
    if not utils.probe_k8s():
        logger.error("failed to connect k8s server")
        sys.exit(1)

    resources = _load_resources(conf.repo)
    resources = _arrange_resources(resources)

    for resource in resources:
        if resource.applier == "k8s":
            k8s.create_or_update(resource, conf.dry_run)
        elif resource.applier == "helm":
            helm.create_or_update(resource, conf.dry_run)
        else:
            logger.error(f"unknown resource applier: {resource.applier}")


def clean(conf):
    # probe k8s
    if not utils.probe_k8s():
        logger.error("failed to connect k8s server")
        sys.exit(1)

    resources = _load_resources(conf.repo)
    resources = _arrange_resources(resources)
    k8s.destroy_unless_exist_in(list(filter(lambda r: r.applier == "k8s", resources)), conf.dry_run)
    helm.destroy_unless_exist_in(list(filter(lambda r: r.applier == "helm", resources)), conf.dry_run)


def list_id(conf):
    resources = _load_resources(conf.repo)
    for resource in resources:
        print(resource.id)


def bench(_):
    # probe k8s
    if not utils.probe_k8s():
        logger.error("failed to connect k8s server")
        sys.exit(1)

    k8s.measure_k8s_operation()


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
    if conf.bench_k8s_get:
        action = bench
    if conf.list_id:
        action = list_id
    if conf.clean:
        action = clean

    return conf, action


def main():
    conf, action = _parse_conf_and_action()
    action(conf)


if __name__ == "__main__":
    main()
