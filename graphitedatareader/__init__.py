#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
GraphiteDataReader
~~~~~~~~~~~~~~~~~~

A module to read data from graphite into a panda Dataframe or Panel object.

To get data from a Graphite instance, do the following:

    from graphitedatareader import GraphiteDataReader

    g = GraphiteDataReader(url='graphite.url.com')
    df = g.read(metrics='graphite.metric.to.fetch', start='-1h', end='now')

"""

from .BaseReader import GraphiteDataError
from .GraphiteDataReader import GraphiteDataReader
from .GraphiteMetricsReader import GraphiteMetricsReader

__all__ = [ 'GraphiteDataReader', 'GraphiteDataError', 'GraphiteMetricsReader' ]

__version__ = '0.2.0'
