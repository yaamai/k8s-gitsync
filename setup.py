from setuptools import setup

setup(
    name="kgs",
    version="0.0.1",
    install_requires=["PyYAML", "toposort"],
    packages=['kgs'],
    entry_points={
        "console_scripts": [
            "kgs = kgs.main:main",
        ]
    }
)
