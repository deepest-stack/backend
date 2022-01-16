#!/usr/local/greenplum-db-6.10.0/ext/python/bin/python
# coding=utf-8


from setuptools import setup, find_packages

setup(
        name='dl',
        version='0.1.0',
        description='Greenplum In-Database DL Tools Based On Keras',
        packages=find_packages(exclude=("debug", )),
)
