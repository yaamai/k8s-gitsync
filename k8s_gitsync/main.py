import sys
import argparse
from toposort import toposort_flatten
from . import utils
from . import k8s
from . import helm
from . import log

logger = log.getLogger(__name__)


def main():
    cli_desc = "Synchronizing the states with the manifests of k8s/helm"
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument("repo", help="manifests repository directory")
    parser.add_argument("--clean", action="store_true", help="clean up the resources removed from manifests")
    parser.add_argument("--list-id", action="store_true", help="show resource id list")
    parser.add_argument("--bench-k8s-get", action="store_true", help="benchmark k8s get operation")
    parser.add_argument("--dry-run", action="store_true", help="dry run (check differences only)")
    conf = parser.parse_args()

    # benchmark subcommand
    if conf.bench_k8s_get:
        k8s._measure_k8s_operation()
        return

    # probe k8s
    if not utils.probe_k8s():
        logger.error("failed to connect k8s server")
        sys.exit(1)

    # find all manifest files
    resources = utils.get_manifest_files(conf.repo)

    # preload k8s manifests and expand multi-document YAML
    expanded_resources = []
    for resource in resources:
        if resource.applier == "k8s":
            expanded_resources.extend(k8s.expand_multi_document_file(resource))
        if resource.applier == "helm":
            expanded_resources.extend(helm.expand(resource))
    resources = expanded_resources

    # preload helm manifests

    # list id subcommand
    if conf.list_id:
        for resource in resources:
            print(resource.id)
        return

    # arrange by dependencies
    dep_graph = {r.id: r.requires for r in resources}
    dep_sorted = toposort_flatten(dep_graph)
    sorted_resources = []
    for resource_id in dep_sorted:
        resource = [r for r in resources if r.id == resource_id]
        if len(resource) == 1:
            sorted_resources.append(resource[0])
    resources = sorted_resources

    # apply or clean
    if not conf.clean:
        for resource in resources:
            if resource.applier == "k8s":
                k8s.create_or_update(resource, conf.dry_run)
            elif resource.applier == "helm":
                helm.create_or_update(resource, conf.dry_run)
            else:
                logger.error(f"unknown resource applier: {resource.applier}")

    else:
        k8s.destroy_unless_exist_in(list(filter(lambda r: r.applier == "k8s", resources)), conf.dry_run)
        helm.destroy_unless_exist_in(list(filter(lambda r: r.applier == "helm", resources)), conf.dry_run)


if __name__ == "__main__":
    main()
