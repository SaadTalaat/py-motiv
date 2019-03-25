#!/usr/bin/env python3
import os
import sys

from setuptools import setup, find_packages

BASE_DIR = os.path.dirname(__file__)
REQS_PATH = os.path.join(BASE_DIR, 'requirements.txt')
VERSION_PATH = os.path.join(BASE_DIR, 'motiv/version.py')

with open(VERSION_PATH, "r") as fh:
    exec(fh.read())

def get_dependencies():
    install_reqs = []
    dep_links = []

    with open(REQS_PATH) as fd:
        reqs = fd.read().splitlines()

        for req in reqs:
            if req.find('+') > -1:
                dep_links.append(req)
            else:
                install_reqs.append(req)
    return install_reqs, dep_links

classifiers = [
    'Operating System :: OS Independent',
    'Natural Language :: English',
    'Intended Audience :: Developers',
    'Intended Audience :: Financial and Insurance Industry',
    'Programming Language :: Python :: 3',
]

INSTALL_REQUIRES, DEP_LINKS = get_dependencies()

setup(
    name="motiv",
    version=__version__,
    author="Saad Talaat",
    author_email="saadtalaat@gmail.com",
    url="https://saadtalaat.com",
    packages=find_packages(exclude=["*test*"]),
    zip_safe=False,
    install_requires= INSTALL_REQUIRES,
    dependency_links = DEP_LINKS,
    test_suite = 'nose.collector',
    platforms = 'POSIX',
    classifiers=classifiers
)

