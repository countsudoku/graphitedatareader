#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" A class, which implements the metric API of graphite """

from __future__ import print_function, absolute_import

from .graphitedatareader import BaseReader

class GraphiteMetricsAPI(BaseReader):
    """
    Creates a GraphiteMetricAPI object, which you can use to request, which
    metrics are available in the graphite cluster

    Arguments:
        url: string
            the base url to the Graphite host
        tls_verify: string or bool
            enable or disable certificate validation. You can als specify the
            path to a certificate or a directory, which must have been
            processed using the c_rehash utily supplied with OppenSSL
            (default: True)
        session: a requests.Session object (default None)
        timeout: float or tuple
    """

    def __init__(self,
                 url,
                 tls_verify='/etc/ssl/certs/',
                 session=None,
                 timeout=30,
                ):
        self._metrics_api = '/metrics'

        super(GraphiteMetricsAPI, self).__init__(
            url=url,
            tls_verify=tls_verify,
            session=session,
            timeout=timeout,
            )

    def find(self, metrics, start, end):
        """
        Finds metrics under a given path.
        """
        raise NotImplementedError

    def expand(self, metrics):
        """
        Expands the given query with matching paths.
        """
        raise NotImplementedError

    def index(self):
        """
        Walks the metrics tree and returns every metric found as a sorted JSON
        array.
        """
        raise NotImplementedError

if __name__ == "__main__":
    print(__doc__)
