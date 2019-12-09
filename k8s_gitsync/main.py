import argparse
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
    parser.add_argument("--bench-k8s-get", action="store_true", help="benchmark k8s get operation")
    parser.add_argument("--dry-run", action="store_true", help="dry run (check differences only)")
    conf = parser.parse_args()

    if conf.bench_k8s_get:
        k8s._measure_k8s_operation()
        return

    resources = utils.get_manifest_files(conf.repo)

    expanded_resources = []
    for resource in resources:
        if resource.applier == "k8s":
            expanded_resources.extend(k8s.expand_multi_document_file(resource))
    expanded_resources.extend(list(filter(lambda r: r.applier == "helm", resources)))
    resources = expanded_resources

    if not conf.clean:
        for resource in resources:
            if resource.applier == "k8s":
                k8s.create_or_update(resource, conf.dry_run)
            elif resources.applier == "helm":
                helm.create_or_update(resource, conf.dry_run)
            else:
                logger.error(f"unknown resource applier: {resource.applier}")

    else:
        k8s.destroy_unless_exist_in(list(filter(lambda r: r.applier == "k8s", resources)), conf.dry_run)
        helm.destroy_unless_exist_in(list(filter(lambda r: r.applier == "helm", resources)), conf.dry_run)


if __name__ == "__main__":
    main()
