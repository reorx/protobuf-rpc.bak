#!/usr/bin/env python
# coding: utf-8

import protobufrpc
from setuptools import setup, find_packages


setup(
    name="protobufrpc",
    version=protobufrpc.__version__,
    packages=find_packages()
)
