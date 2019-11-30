import argparse
import os.path
import utils
import k8s


def main():
    cli_desc = "Synchronizing the states with the manifests of k8s/helm"
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument("repo", help="manifests repository directory")
    conf = parser.parse_args()

    k8s_manifests, helm_manifests = utils.get_manifest_files(conf.repo)

    for manifest_dir, manifest_files in k8s_manifests.items():
        for manifest_file in manifest_files:
            k8s.create_or_update(os.path.join(manifest_dir, manifest_file))


if __name__ == "__main__":
    main()
