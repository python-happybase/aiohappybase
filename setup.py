import versioneer
from setuptools import setup

setup(
    version=versioneer.get_version(),
    install_requires=[*filter(None, map(str.strip, open('requirements.txt')))],
    cmdclass=versioneer.get_cmdclass(),
)
