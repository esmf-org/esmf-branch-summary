#!/usr/bin/env python
# -*- coding: utf-8 -*-

# pylint: disable=missing-module-docstring

import os
import sys
import codecs
import subprocess

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = "\n" + f.read()


if sys.argv[-1] == "publish":
    subprocess.call(f"{sys.executable} setup.py sdist bdist_wheel upload", shell=False)
    sys.exit()

required = [""]

setup(
    name="esmf-branch-summary",
    version="1.0.0",
    description="CLI tool for generating summary data of testing ESMF framework",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Ryan Long",
    author_email="ryan.long@noaa.gov",
    url="",
    py_modules=["esmf-branch-summary"],
    install_requires=["esmf-git", "tabulate"],
    tests_require=["pytest"],
    license="MIT",
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    extras_require={  # Optional
        "dev": [
            "check-manifest",
            "black",
            "pylint",
            "pytest",
            "pytest-cov",
            "pytest-xdist",
            "tox",
            "bump2version",
        ],
        "test": ["pytest", "pytest-cov", "pytest-forked", "pytest-xdist", "tox"],
    },
    entry_points={
        "console_scripts": [
            "esmf-branch-summary = esmf_branch_summary:main",
        ],
    },
)
