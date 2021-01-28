import versioneer
from setuptools import setup

setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    install_requires=[*filter(None, map(str.strip, open('requirements.txt')))],
)
