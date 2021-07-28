# -*- coding: utf-8 -*-
from json import dumps
from typing import List
from requests import Session, Response
import re
import logging
from ijson import parse
from ijson.common import ObjectBuilder
from itertools import chain, islice
from datetime import date, time, datetime
from time import gmtime
from . import api_globals
from .api_exceptions import AuthError, DatabaseError, ProgrammingError, \
    CursorClosedException, ConnectionClosedException

apilevel = '2.0'
threadsafety = 3
paramstyle = 'qmark'
default_storage_plugin = ''

logger = logging.getLogger('drilldbapi')

# Python DB API 2.0 classes


class Cursor(object):

    @staticmethod
    def substitute_in_query(string_query, parameters):
        query = string_query
        try:
            for param in parameters:
                if type(param) == str:
                    param = f"'{param}'"

                query.replace('?', param, 1)
                logger.debug(f'set parameter value {param}')
        except Exception as ex:
            logger.error(f'query parameter substitution encountered {ex}.')
            raise ProgrammingError(
                'Could not substitute query parameter values',
                None
            )

        return query

    def __init__(self, conn):

        self.arraysize: int = 200
        self.description: tuple = None
        self.connection = conn
        self.rowcount: int = -1
        self.rownumber: int = None
        self.result_md = {}

        self._is_open: bool = True
        self._result_event_stream = self._row_stream = None
        self._typecaster_list: list = None

    def is_open(func):
        '''Decorator for methods which require a connection'''

        def func_wrapper(self, *args, **kwargs):
            if self._is_open is False:
                raise CursorClosedException(
                    f'Cannot call {func} with a closed cursor.'
                )
            elif self.connection._connected is False:
                raise ConnectionClosedException(
                    f'Cannot call {func} with a closed connection.'
                )
            else:
                return func(self, *args, **kwargs)

        return func_wrapper

    def _gen_description(self, col_types):
        blank = [None] * len(self.result_md['columns'])
        self.description = tuple(
            zip(
                self.result_md['columns'],  # name
                col_types or blank,  # type_code
                blank,  # display_size
                blank,  # internal_size
                blank,  # precision
                blank,  # scale
                blank   # null_ok
            )
        )

    def _report_query_state(self):
        md = self.result_md
        query_state = md.get('queryState', None)
        logger.info(
            f'received final query state {query_state}.'
        )

        if query_state != 'COMPLETED':
            logger.warning(
                md.get(
                    'exception',
                    'No exception returned, c.f. drill.exec.http.rest.errors.verbose.'
                )
            )
            logger.warning(
                md.get('errorMessage', 'No error message returned.'))
            logger.warning(md.get('stackTrace', 'No stack trace returned.'))

            raise DatabaseError(
                f'Final Drill query state is {query_state}',
                None
            )

    def _outer_parsing_loop(self) -> bool:
        '''Internal method to process the outermost query result JSON structure.

        This loop will parse result JSON, recording metadata as it goes, until
        it either encounters row data or the end of the result stream.  If row
        data is encountered then parsing is halted in order that it can be driven
        in a streaming fashion by the user making calls to the fetchN() methods.

        Since there is also result metadata found _after_ row data, the fetchN()
        methods should start this loop again once they've encountered the end of
        the row data.

        Returns True iff row data is encountered in the result.
        '''
        try:
            while True:
                prefix, event, value = next(self._result_event_stream)
                logger.debug(f'ijson parsed {prefix}, {event}, {value}')

                if event != 'map_key':
                    continue

                if value == 'rows':
                    self._row_stream = _items_once(
                        self._result_event_stream, 'rows.item'
                    )
                    # stop here so that row parsing can be driven by user calls
                    # to fetchN
                    return True
                else:
                    # save the parsed object to the result metadata dict
                    self.result_md[value] = next(
                        _items_once(self._result_event_stream, value)
                    )
        except StopIteration:
            logger.info(
                'reached the end of the result stream, parsing complete.'
            )

        self._report_query_state()
        return False

    @is_open
    def getdesc(self):
        return self.description

    @is_open
    def close(self):
        self._is_open = False
        if self._row_stream is not None:
            self._row_stream.close()
            self._row_stream = None
            logger.debug('closed row data stream.')
        else:
            logger.debug('had no row data stream to close.')

    @is_open
    def execute(self, operation, parameters=()):
        if self._row_stream:
            logger.warning(
                'will close the existing row data stream.'
            )
            self._row_stream.close()

        self.rowcount = -1
        self.rownumber = 0

        matchObj = re.match(r'^SHOW FILES FROM\s(.+)',
                            operation, re.IGNORECASE)
        if matchObj:
            self._default_storage_plugin = matchObj.group(1)
            logger.debug(
                'sets the default storage plugin to '
                f'{self._default_storage_plugin}'
            )

        resp = self.connection.submit_query(
            self.substitute_in_query(operation, parameters)
        )

        if resp.status_code != 200:
            err_msg = resp.json().get('errorMessage', None)
            raise ProgrammingError(err_msg, resp.status_code)

        self._result_event_stream = parse(RequestsStreamWrapper(resp))
        row_data_present = self._outer_parsing_loop()
        # The leading result metadata has now been parsed.

        logger.info(
            f'received Drill query ID {self.result_md.get("queryId", None)}.'
        )

        if not row_data_present:
            return

        cols = self.result_md['columns']
        # Column metadata could be trailing or entirely absent
        if 'metadata' in self.result_md:
            md = self.result_md['metadata']
            # strip size information from column types e.g. VARCHAR(10)
            basic_coltypes = [re.sub(r'\(.*\)', '', m) for m in md]
            self._gen_description(basic_coltypes)

            self._typecaster_list = [
                self.connection.typecasters.get(col, lambda v: v) for
                col in basic_coltypes
            ]
        else:
            self._gen_description(None)
            logger.warn(
                'encountered data before metadata, typecasting during '
                'streaming by this module will not take place.  Upgrade '
                'to Drill >= 1.19 or apply your own typecasting.'
            )

        logger.info(f'opened a row data stream of {len(cols)} columns.')

    @is_open
    def executemany(self, operation, seq_of_parameters):
        for parameters in seq_of_parameters:
            logger.debug(f'executes with parameters {parameters}.')
            self.execute(operation, parameters)

    @is_open
    def fetchone(self):
        res = self.fetchmany(1)
        return next(iter(res), None)

    @is_open
    def fetchmany(self, size: int = None):
        '''Fetch the next set of rows of a query result.

        The number of rows to fetch per call is specified by the size
        parameter. If it is not given, the cursor's arraysize determines the
        number of rows to be fetched. If size is negative then all remaining
        rows are fetched.
        '''
        if self._row_stream is None:
            raise ProgrammingError(
                'has no row data, have you executed a query that returns data?',
                None
            )

        fetch_until = self.rownumber + (size or self.arraysize)
        results = []

        try:
            while self.rownumber != fetch_until:
                row_dict = next(self._row_stream)
                # values ordered according to self.result_md['columns']
                row = [row_dict[col] for col in self.result_md['columns']]

                if self._typecaster_list is not None:
                    row = (f(v) for f, v in zip(self._typecaster_list, row))

                results.append(tuple(row))
                self.rownumber += 1

                if self.rownumber % api_globals._PROGRESS_LOG_N == 0:
                    logger.info(f'streamed {self.rownumber} rows.')

        except StopIteration:
            self.rowcount = self.rownumber
            logger.info(
                f'reached the end of the row data after {self.rownumber}'
                ' records.'
            )
            # restart the outer parsing loop to collect trailing metadata
            self._outer_parsing_loop()

        return results

    @is_open
    def fetchall(self) -> List:
        '''Fetch all (remaining) rows of a query result.'''
        return self.fetchmany(-1)

    def setinputsizes(sizes):
        '''Not supported.'''
        logger.warn('setinputsizes is a no-op in this driver.')

    def setoutputsize(size, column=0):
        '''Not supported.'''
        logger.warn('setoutputsize is a no-op in this driver.')

    @is_open
    def get_query_id(self) -> str:
        """Unofficial convenience method for getting the Drill ID of the last query.
        """
        return self._query_id

    @is_open
    def get_column_names(self) -> List:
        """Unofficial convenience method for getting the column names."""
        return [d[0] for d in self.description]

    @is_open
    def get_query_metadata(self) -> List:
        """Unofficial convenience method for getting the column metadata."""
        return [d[1] for d in self.description]

    def get_default_plugin(self) -> str:
        """Unofficial convenience method for getting the default storage plugin.
        """
        return self._default_storage_plugin

    # Make this Cursor object iterable

    def __next__(self):
        return self.fetchone()

    def __iter__(self):
        return self


class Connection(object):
    def __init__(self,
                 host: str,
                 port: int,
                 proto: str,
                 session: Session):
        if session is None:
            raise ProgrammingError('A Requests session is required.', None)

        self._base_url = f'{proto}{host}:{port}'
        self._session = session
        self._connected = True

        logger.debug('queries Drill\'s version number...')
        resp = self.submit_query(
            'select min(version) version from sys.drillbits'
        )
        self.drill_version = resp.json()['rows'][0]['version']
        logger.info(f'has connected to Drill version {self.drill_version}.')

        if self.drill_version < '1.19':
            self.typecasters = {}
        else:
            # Starting in 1.19 the Drill REST API returns UNIX times
            self.typecasters = {
                'DATE': lambda v: DateFromTicks(v/1000),
                'TIME': lambda v: TimeFromTicks(v/1000),
                'TIMESTAMP': lambda v: TimestampFromTicks(v/1000)
            }
            logger.debug(
                'sets up typecasting functions for Drill >= 1.19.'
            )

    def submit_query(self, query: str):
        payload = api_globals._PAYLOAD.copy()
        # TODO: autoLimit, defaultSchema
        payload['query'] = query

        logger.debug('sends an HTTP POST with payload')
        logger.debug(payload)

        return self._session.post(
            f'{self._base_url}/query.json',
            data=dumps(payload),
            headers=api_globals._HEADER,
            timeout=None,
            stream=True
        )

    # Decorator for methods which require connection

    def connected(func):

        def func_wrapper(self, *args, **kwargs):
            if not self._connected:
                raise ConnectionClosedException(
                    f'Connection object is closed when calling {func}'
                )

            return func(self, *args, **kwargs)

        return func_wrapper

    def is_connected(self):
        return self._connected

    @connected
    def close(self):
        try:
            self._session.close()
            self._connected = False
        except Exception as ex:
            logger.warn(f'encountered {ex} when try to close connection.')
            raise ConnectionClosedException('Failed to close connection')

    @connected
    def commit(self):
        logger.info('A commit is a no-op in this driver.')

    @connected
    def cursor(self) -> Cursor:
        return Cursor(
            self
        )


def connect(host: str,
            port: int = 8047,
            db: str = None,
            use_ssl: bool = False,
            drilluser: str = None,
            drillpass: str = None,
            verify_ssl: bool = False,
            ca_certs: bool = None,
            ) -> Connection:

    session = Session()

    if verify_ssl is False:
        session.verify = False
    else:
        if ca_certs is not None:
            session.verify = ca_certs
        else:
            session.verify = True

    proto = 'https://' if use_ssl in [True, 'True', 'true'] else 'http://'
    base_url = f'{proto}{host}:{port}'

    if drilluser is None:
        payload = api_globals._PAYLOAD.copy()
        payload['query'] = 'show schemas'
        response = session.post(
            f'{base_url}/query.json',
            data=dumps(payload),
            headers=api_globals._HEADER
        )
    else:
        payload = api_globals._LOGIN.copy()
        payload['j_username'] = drilluser
        payload['j_password'] = drillpass
        response = session.post(
            f'{base_url}/j_security_check',
            data=payload
        )

    if response.status_code != 200:
        logger.error('was unable to connect to Drill.')
        raise DatabaseError(
            str(response.json().get('errorMessage', None)),
            response.status_code
        )

    raw_data = response.text
    if raw_data.find('Invalid username/password credentials') >= 0:
        logger.error('failed to authenticate to Drill.')
        raise AuthError(str(raw_data), response.status_code)

    conn = Connection(host, port, proto, session)
    if db is not None:
        conn.submit_query(f'USE {db}')

    return conn


class RequestsStreamWrapper(object):
    """
    A wrapper around a Requests response payload for converting
    the returned generator into a file-like object.
    """

    def __init__(self, resp: Response):
        self.data = chain.from_iterable(resp.iter_content())

    def read(self, n):
        return bytes(islice(self.data, None, n))


def _items_once(event_stream, prefix):
    '''
    Generator dispatching native Python objects constructed from the ijson
    events under the next occurrence of the given prefix.  It is very
    similar to ijson.items except that it will not consume the entire JSON
    stream looking for occurrences of prefix, but rather stop after
    completing the *first* encountered occurrence of prefix.  The need for
    this property is what precluded the use of ijson.items instead.
    '''
    current = None
    while current != prefix:
        current, event, value = next(event_stream)

    logger.debug(f'found and will now parse an occurrence of {prefix}')
    while current == prefix:
        if event in ('start_map', 'start_array'):
            object_depth = 1
            builder = ObjectBuilder()
            while object_depth:
                builder.event(event, value)
                current, event, value = next(event_stream)
                if event in ('start_map', 'start_array'):
                    object_depth += 1
                elif event in ('end_map', 'end_array'):
                    object_depth -= 1
            del builder.containers[:]
            yield builder.value
        else:
            yield value

        current, event, value = next(event_stream)
    logger.debug(f'finished parsing one occurrence of {prefix}')


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1


# Mandatory type objects defined by DB-API 2 specs.

STRING = DBAPITypeObject('VARCHAR')
BINARY = DBAPITypeObject('BINARY', 'VARBINARY')
NUMBER = DBAPITypeObject('FLOAT4', 'FLOAT8', 'SMALLINT',
                         'INT', 'BIGINT', 'DECIMAL')
DATETIME = DBAPITypeObject('DATE', 'TIMESTAMP')
ROWID = DBAPITypeObject()

# Additional type objects (more specific):

BOOL = DBAPITypeObject('BIT')
SMALLINT = DBAPITypeObject('SMALLINT')
INTEGER = DBAPITypeObject('INT')
LONG = DBAPITypeObject('BIGINT')
FLOAT = DBAPITypeObject('FLOAT4', 'FLOAT8')
NUMERIC = DBAPITypeObject('VARDECIMAL')
DATE = DBAPITypeObject('DATE')
TIME = DBAPITypeObject('TIME')
TIMESTAMP = DBAPITypeObject('TIMESTAMP')
INTERVAL = DBAPITypeObject('INTERVALDAY', 'INTERVALYEAR')

# Mandatory type helpers defined by DB-API 2 specs


def Date(year, month, day):
    """Construct an object holding a date value."""
    return date(year, month, day)


def Time(hour, minute=0, second=0, microsecond=0, tzinfo=None):
    """Construct an object holding a time value."""
    return time(hour, minute, second, microsecond, tzinfo)


def Timestamp(year, month, day, hour=0, minute=0, second=0, microsecond=0,
              tzinfo=None):
    """Construct an object holding a time stamp value."""
    return datetime(year, month, day, hour, minute, second, microsecond,
                    tzinfo)


def DateFromTicks(ticks):
    """Construct an object holding a date value from the given ticks value."""
    return Date(*gmtime(ticks)[:3])


def TimeFromTicks(ticks):
    """Construct an object holding a time value from the given ticks value."""
    return Time(*gmtime(ticks)[3:6])


def TimestampFromTicks(ticks):
    """Construct an object holding a timestamp from the given ticks value."""
    return Timestamp(*gmtime(ticks)[:6])


class Binary(bytes):
    """Construct an object capable of holding a binary (long) string value."""
