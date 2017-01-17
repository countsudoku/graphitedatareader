#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from graphitedatareader import __version__

setup(
    name='graphitedatareader',
    packages=['graphitedatareader',],
    version=__version__,
    description='Read data from GRaphite into a pandas DataFrame or Panel',
    author='Moritz C.K.U. Schneider',
    author_email='schneider.moritz@gmail.com',
    url='https://github.com/countsudoku/graphitedatareader',
    keywords=['graphite' ],
    license='BSD License',
    install_requires=['pandas', 'requests'],
)
