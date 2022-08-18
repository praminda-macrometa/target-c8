#!/usr/bin/env python
from setuptools import setup

with open("README.md", "r", encoding="utf-8") as f:
    readme = f.read()

setup(
    name='target-c8',
    version='0.1.4',
    description='Singer.io target for writing JSON Line files',
    long_description=readme,
    long_description_content_type='text/markdown',
    author='Andy Huynh',
    author_email="andy.huynh312@gmail.com",
    url="https://github.com/praminda-macrometa/target-c8",
    keywords=["singer", "singer.io", "target", "etl"],
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['target_c8'],
    install_requires=['jsonschema==2.6.0', 'singer-python==5.8.0', 'adjust-precision-for-schema==0.3.3'],
    entry_points='''
          [console_scripts]
          target-c8=target_c8:main
      ''',
)
