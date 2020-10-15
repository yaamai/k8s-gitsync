from setuptools import find_packages
from setuptools import setup

setup(
    name="kgs",
    version="0.0.1",
    install_requires=["PyYAML", "toposort", "typing-extensions"],
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "kgs = kgs.main:main",
        ]
    },
)
