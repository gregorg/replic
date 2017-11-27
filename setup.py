#!/usr/bin/env python3
# -*- coding: iso-8859-1 -*-
# vim: ai ts=4 sts=4 et sw=4
from setuptools import setup

setup(
    name="replic",
    version="1.0",
    license="MIT",
    author="Grégory Duchatelet",
    author_email="skygreg@gmail.com",
    maintainer="Grégory Duchatelet",
    maintainer_email="skygreg@gmail.com",
    install_requires=["termcolor","mysql.connector", "psutil"],
    description="MariaDB replication checks and master switch",
    url="https://github.com/gregorg/replic",
    packages=[],
    scripts=["replic.py"]
)
