#!/usr/bin/env python3
# coding:utf-8
import subprocess
from setuptools import setup, find_packages


def get_git_revision_short_hash():
    return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD'])


setup(
    name='arupy',
    # See https://www.python.org/dev/peps/pep-0440/
    version="1.{}".format(get_git_revision_short_hash()),
    description='Arupy - RabbitMQ Client APP',
    author='v1ll4n',
    author_email='v1ll4n@qq.com',
    url='http://.com',
    install_requires=[
        "pyyaml",
        "pika",
        "colorama",
        "pydantic",
    ],
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
)
