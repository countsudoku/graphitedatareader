#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='graphitedatareader',
    packages=['graphitedatareader',],
    version='0.1.0',
    description='Read data from GRaphite into a pandas DataFrame or Panel',
    author='Moritz C.K.U. Schneider',
    author_email='schneider.moritz@gmail.com',
    url='https://github.com/countsudoku/graphitedatareader',
    keywords=['graphite' ],
    license='BSD License',
    install_requires=['pandas', 'requests'],
)
