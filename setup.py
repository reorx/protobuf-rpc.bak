#!/usr/bin/env python
# coding: utf-8

from setuptools import setup, find_packages

import protobufrpc


setup(
    name="protobufrpc",
    version=protobufrpc.__version__,
    packages=find_packages()
)
