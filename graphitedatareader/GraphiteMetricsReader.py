# -*- coding: utf-8 -*-

""" A class to explore Mtric path in Graphite """

from __future__ import print_function, absolute_import

from .BaseReader import GraphiteDataError
from .MetricsAPI import GraphiteMetricsAPI

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
                 tls_verify='/etc/ssl/certs/',
                 session=None,
                 timeout=30.,
                ):

        self.metrics_obj = GraphiteMetricsAPI(
            url=url,
            tls_verify=tls_verify,
            session=session,
            timeout=timeout,
        )

    def walk(self, top='', start=None, end=None):
        """ Walks the metrics tree like os.walk

        Arguments:
            base_path: string
                the target, where the walk starts
            start: string
                A start time (see graphite documentation for the format)
            end: string
                A end date (same as start)

        Reurns:
            a generator object, which yields (target, non-leafs, leafs) for
            each metric.
        """
        if top == '':
            path = '*'
        else:
            path = top + '.*'
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
