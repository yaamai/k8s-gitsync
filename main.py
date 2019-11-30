import argparse
import utils


def main():
    cli_desc = "Synchronizing the states with the manifests of k8s/helm"
    parser = argparse.ArgumentParser(description=cli_desc)
    parser.add_argument("repo", help="manifests repository directory")
    conf = parser.parse_args()

    k8s_manifests, helm_manifests = utils.get_deploy_config(conf.repo)


if __name__ == "__main__":
    main()
