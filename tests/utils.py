import os

import yaml


# simply support bytes type in yaml
yaml.SafeLoader.add_constructor("!bytes", lambda _1, n: n.value.encode())


def load_testdata(module_file_path: str) -> dict:
    data_path = os.path.join(os.path.dirname(os.path.abspath(module_file_path)), "data")
    filename = os.path.splitext(os.path.basename(module_file_path))[0] + ".yaml"
    path = os.path.join(data_path, filename)
    with open(path, "r") as f:
        return yaml.safe_load(f)
