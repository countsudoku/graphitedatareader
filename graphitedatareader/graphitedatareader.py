# -*- coding: utf-8 -*-

""" A class to get Data from Graphite """

from __future__ import print_function

import urlparse
import warnings

from pandas import read_csv, MultiIndex, concat, Panel
from pandas.compat import StringIO, string_types
import requests

class GraphiteDataError(IOError):
    """ A error class, for all kind of exceptions for GraphiteDataReader """
    pass

class GraphiteDataReader(object):
    """
    Creates a GraphitDataReader object, which you can use to read different
    metrics in a pandas DataFrame

    Arguments:
        url (string): the base url to the Graphite host
        start (string): the starting date timestamp.
            All Graphite datestrings are allowed
        end (string): same as start date
        tls_verify: enable or disable certificate validation. You can als
            specify the path to a certificate or a directory, which must have
            been processed using the c_rehash utily supplied with OppenSSL
            (default: True)
        session: a requests.Session object (default None)
    """
    url = None
    def __init__(self,
                 url=None,
                 start=None,
                 end=None,
                 tls_verify='/etc/ssl/certs/',
                 session=None,
                ):

        if url:
            self.url = url
        self._from = start
        self._until = end
        self._verify = tls_verify

        self._session = self._init_session(session)
        self._format = 'csv'
        self._render_api = '/render'

    @property
    def start(self):
        """ A property to set the start date from which the metric is fetched """
        return self._from
    @start.setter
    def start(self, start):
        self._from = start

    @property
    def end(self):
        """ A property to set the end date from which the metric is fetched """
        return self._until
    @end.setter
    def end(self, end):
        self._until = end

    def read(self,
             metrics,
             start=None,
             end=None,
             create_multiindex=True,
             remove_duplicate_indices=True,
            ):
        """ read the data from Graphite

        Arguments:
            metric (string): the metrics you want to look up
        start (string): the starting date timestamp.  All Graphite datestrings
            are allowed
        end (string): the ending date timestamp, same as start date
        smart_alias (bool): using only the metrics, which differs as table
            columns (default True)

        returns:
            a pandas DataFrame or Panel with the requested Data from Graphite
        """
        # sanity checks
        if not self.url:
            raise GraphiteDataError('No URL specified')
        else:
            url = urlparse.urljoin(self.url, self._render_api)

        if start is None:
            start = self._from

        if end is None:
            end = self._until

        if isinstance(metrics, string_types):
            df = self._download_single_metric(url, metrics, start, end)
            if create_multiindex:
                df.columns = self._create_multiindex(
                    df.columns,
                    remove_duplicate_indices
                    )
        elif isinstance(metrics, list):
            dfs = []
            for metric in metrics:
                dfs.append(self._download_single_metric(url, metric, start, end))
            df = concat(dfs, axis=1)
            if create_multiindex:
                df.columns = self._create_multiindex(
                    df.columns,
                    remove_duplicate_indices
                    )
        elif isinstance(metrics, dict):
            warnings.warn('To create a Panel from a dict of metric is a '
                          'experimental feature. So don\'t use this in '
                          'production! Because the resulting object may be '
                          'changed in the future or the feature may be removed.')
            dfs = {}
            for label, metric in metrics.items():
                dfs[label] = self._download_single_metric(url, metric, start, end)
                if create_multiindex:
                    dfs[label].columns = self._create_multiindex(
                        dfs[label].columns,
                        remove_duplicate_indices,
                        )
            df = Panel.from_dict(dfs)
        else:
            raise TypeError

        return df

    def _download_single_metric(self, url, target, start, end):
        """ downloads of the specified target

        Arguments:
            url (string): The Graphite render url
            target (string): The metric you want do download

        returns:
            a pandas.DataFrame
        """
        params = { 'target': target,
                   'from': start,
                   'until': end,
                   'format': self._format, }
        r = self._session.get(url, params=params, verify=self._verify)
        if r.status_code != requests.codes.ok:
            raise  GraphiteDataError(
                'Unable to read URL: {url}'
                .format(url=url)
                )

        df = read_csv( StringIO(r.text),
                       names=['metric', 'datetime', 'data'],
                       parse_dates=['datetime'],
                       index_col=['metric', 'datetime'],
                       squeeze=False,
                     ).unstack('metric')['data']
        return df

    @staticmethod
    def _init_session(session):
        """ create a default Session if no session is specified """
        if session is None:
            session = requests.Session()
        return session


    @staticmethod
    def _create_multiindex(idx, remove_duplicates=False):
        """ Tries to find the field that differs in a MultiIndex and remove
        all other column levels"""

        # split the metrics on a dot
        columns = [ column.split('.') for column in idx.values ]
        row_idx = []

        # padding
        max_length = 0
        for column in columns:
            max_length = max(max_length, len(column))
        for column in columns:
            if len(column) < max_length:
                column.extend(['' for _ in range(max_length - len(column)) ])

        # check, which metric fields differ
        if remove_duplicates:
            for index, column in enumerate(columns[:-1]):
                for sec_column in columns[index+1:]:
                    for idx, names in enumerate(zip(column, sec_column)):
                        if names[0] != names[1] and idx not in row_idx:
                            row_idx.append(idx)
            row_idx.sort()
            new_columns = []
            for column in columns:
                new_columns.append([ column[idx] for idx in row_idx])
        else:
            new_columns = columns

        return MultiIndex.from_tuples(new_columns).sort_values()

    @staticmethod
    def add_index_level(idx, level):
        """ Adds a level to a MultiIndex

        Params:
            idx: pandas.Index
                The Index to add a level
            level: string
                The levels you want to add

        Returns:
            A new index object with the added level
        """

        if isinstance(level, string_types):
            new_levels = [[level]]
        else:
            raise TypeError
        for level in idx.levels:
            new_levels.append(list(level.values))
        return MultiIndex.from_product(new_levels)

if __name__ == "__main__":
    print(__doc__)
