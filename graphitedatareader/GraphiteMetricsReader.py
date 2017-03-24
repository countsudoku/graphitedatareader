# -*- coding: utf-8 -*-

""" A class to explore Mtric path in Graphite """

from __future__ import print_function, absolute_import

import re
from itertools import chain

from .BaseReader import GraphiteDataError
from .GraphiteMetricsAPI import GraphiteMetricsAPI

class GraphiteMetricsReader(object):
    """
    Creates a GraphiteDataReader object, which you can use to read different
    metrics in a pandas DataFrame

    Arguments:
        url (string): the base url to the Graphite host
        tls_verify: enable or disable certificate validation. You can als
            specify the path to a certificate or a directory, which must have
            been processed using the c_rehash utily supplied with OppenSSL
            (default: True)
        session: a requests.Session object (default None)
        timeout: float or tuple
            a request timeout, see requests doc for more information
    """
    def __init__(self,
                 url,
                 base_path=None,
                 tls_verify='/etc/ssl/certs/',
                 session=None,
                 timeout=30.,
                ):

        if not base_path:
            self.base_path = ''
        else:
            self.base_path = base_path
        self.url = url
        self.tls_verify = tls_verify
        self.session = session
        self.timeout = timeout

        self.metrics_obj = GraphiteMetricsAPI(
            url=url,
            tls_verify=tls_verify,
            session=session,
            timeout=timeout,
        )

        self._leafs = None
        self._internal_nodes = None


    def __dir__(self):
        if not self._leafs:
            self._fill_nodes()
        return dir(type(self)) + self._leafs + self._internal_nodes

    def __getattr__(self, name):
        if not self._leafs:
            self._fill_nodes()
        if name in self._leafs:
            return self.base_path + '.' + name
        elif name in self._internal_nodes:
            return GraphiteMetricsReader(
                self.url,
                self.base_path + '.' + name,
                self.tls_verify,
                self.session,
                self.timeout)

    def _fill_nodes(self):
        w = self.walk()
        metrics = w.next()[1:]
        w.close()
        self._internal_nodes = []
        for node in metrics[0]:
            self._internal_nodes.append(
                node.replace(self.base_path + '.', '', 1))
        self._leafs = []
        for leaf in metrics[1]:
            self._leafs.append(
                leaf.replace(self.base_path + '.', '', 1))

    def walk(self, top=None, start=None, end=None):
        """ Walks the metrics tree like os.walk

        Arguments:
            top: string
                the target, where the walk starts
            start: string
                A start time (see graphite documentation for the format)
            end: string
                A end date (same as start)

        Reurns:
            a generator object, which yields (target, non-leafs, leafs) for
            each metric.
        """
        if top:
            full_path = self.base_path + '.' + top
        else:
            full_path = self.base_path

        if full_path == '':
            path = '*'
        else:
            path = full_path + '.*'

        metrics = self.metrics_obj.find(path, start, end)
        leafs = set()
        internal_nodes = set()
        for metric in metrics:
            if metric['leaf'] == 0:
                internal_nodes.add(metric['id'])
            elif metric['leaf'] == 1:
                leafs.add(metric['id'])
            else:
                raise GraphiteDataError('Unknown metrics format')

        yield (top, list(internal_nodes), list(leafs))
        for node in internal_nodes:
            for branch in self.walk(node, start, end):
                yield branch

    def list(self, path='', start=None, end=None):
        """ lists the content of a path (leafs and non-leafs)

        Arguments:
            path: string
                the metrics path, where the conten should be listed
            start: string
                A start time (see graphite documentation for the format)
            end: string
                A end date (same as start)

        Reurns:
            a generator object, which yields (target, non-leafs, leafs) for
            each metric.
        """
        g_walk = self.walk(path, start, end)
        path_content = g_walk.next()
        g_walk.close()
        return list(chain(path_content[1], path_content[2]))

