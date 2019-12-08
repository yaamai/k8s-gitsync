import argparse
import os.path
from . import utils
from . import k8s
from . import helm


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

    k8s_manifests, helm_manifests = utils.get_manifest_files(conf.repo)

    if not conf.clean:
        for manifest_dir, manifest_files in k8s_manifests.items():
            for manifest_file in manifest_files:
                k8s.create_or_update(os.path.join(manifest_dir, manifest_file), conf.dry_run)

        helm.create_or_update(helm_manifests, conf.dry_run)
    else:
        filepaths = []
        for manifest_dir, manifest_files in k8s_manifests.items():
            for manifest_file in manifest_files:
                filepaths.append(os.path.join(manifest_dir, manifest_file))

        k8s.destroy_unless_exist_in(filepaths, conf.dry_run)
        helm.destroy_unless_exist_in(helm_manifests, conf.dry_run)


if __name__ == "__main__":
    main()
