from setuptools import setup, find_packages
from mypy import List


def read_dependencies() -> List:
    with open("requirements.txt", "r") as reqs:
        requirements = []
        for line in reqs.readlines():
            requirements.append(line)
        return requirements


setup(
    name="FPL",
    version="0.0.1",
    author="Ossian Hempel",
    author_email="hemposse@hotmail.com",
    description="",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/ossianhempel/fpl",
    packages=find_packages(),
    install_requires=read_dependencies(),
    python_requires=">=3.7",  # min python version required
)
