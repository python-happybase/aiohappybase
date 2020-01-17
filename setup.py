from pathlib import Path
from setuptools import setup

__version__ = None
exec(Path('aiohappybase/_version.py').read_text())

setup(
    version=__version__,
    install_requires=[
        line for line in
        map(str.strip, Path('requirements.txt').read_text().splitlines())
        if line
    ],
)
