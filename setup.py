from setuptools import setup

setup(
    name="k8s_gitsync",
    version="0.0.1",
    install_requires=["PyYAML", "toposort"],
    packages=['k8s_gitsync'],
    entry_points={
        "console_scripts": [
            "k8s-gitsync = k8s_gitsync.main:main",
        ]
    }
)
