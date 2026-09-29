# -*- coding: utf-8 -*-
"""
This module provides the implementation of the Cursor object for interfacing with
the Drill database. It adheres to the Python DB API 2.0 standard, enabling execution
of SQL queries, retrieval of data, and management of cursor states.

This module fetches and processes query results from Drill, supporting both
metadata extraction and data streaming for enhanced query handling.

Classes:
- Cursor: Encapsulates the functionality for executing SQL statements, retrieving
  query results, and maintaining database connection integrity.
"""
import logging
import re
from datetime import date, time, datetime, timedelta
from decimal import Decimal
from itertools import chain, islice
from json import dumps
from math import isfinite
from numbers import Integral, Real
from time import sleep
from typing import List
from urllib.parse import quote

from ijson import parse
from ijson.common import ObjectBuilder
from requests import Session, Response
from requests.exceptions import RequestException, SSLError, Timeout
from uuid import uuid4

from . import api_globals
from .api_exceptions import (
    AuthError,
    ConnectionClosedException,
    CursorClosedException,
    DatabaseError,
    DrillWarning,
    Error,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    TransportError,
)

def _transport_error(ex, what):
    """Wrap a requests failure as a DB-API OperationalError subclass."""
    return TransportError(f'Drill REST {what} failed: {type(ex).__name__}: {ex}', None)


# Leading block comment added to every cursor statement. It identifies the
# statement in Drill's running-query list, which is the only way to cancel a
# REST query before Drill sends its query ID with the first result batch.
_QUERY_TAG_PREFIX = 'sqlalchemy-drill:'
_QUERY_TAG_RE = re.compile(
    r'\A/\* sqlalchemy-drill:(?P<tag>[0-9a-f]{32})(?: group:(?P<group>[0-9a-f]{32}))? \*/ ')


# DB-API 2.0 requires Warning to be exported at module level
# We renamed it to DrillWarning to avoid shadowing built-in, but alias it here
Warning = DrillWarning  # noqa: A001  # pylint: disable=redefined-builtin

# Explicit exports for DB-API 2.0 compliance
__all__ = [
    # Module globals
    'apilevel', 'threadsafety', 'paramstyle',
    # Classes
    'Connection', 'Cursor',
    # Functions
    'connect',
    # Type objects
    'STRING', 'BINARY', 'NUMBER', 'DATETIME', 'ROWID',
    'BOOL', 'SMALLINT', 'INTEGER', 'LONG', 'FLOAT', 'NUMERIC',
    'DATE', 'TIME', 'TIMESTAMP', 'INTERVAL',
    # Type constructors
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks', 'Binary',
    # Exceptions (DB-API 2.0 required)
    'Warning', 'Error', 'InterfaceError', 'DatabaseError', 'OperationalError',
    'IntegrityError', 'InternalError', 'ProgrammingError', 'NotSupportedError',
    # Additional exceptions
    'AuthError', 'CursorClosedException', 'ConnectionClosedException',
]

apilevel = '2.0'
threadsafety = 3
paramstyle = 'qmark'
default_storage_plugin = ''

logger = logging.getLogger('drilldbapi')

# Python DB API 2.0 classes


class Cursor:

    @staticmethod
    def _sql_literal(value):
        """Render a DB-API parameter as one Drill SQL literal.

        Values that Drill cannot represent as a literal are rejected here
        rather than rendered into SQL that is guaranteed to fail server side.
        """
        if value is None:
            return 'NULL'
        if isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        if isinstance(value, (bytes, bytearray, memoryview)):
            # Drill 1.21.2 parses X'..' but cannot evaluate it:
            # "Unable to convert the value of X'deadbeef':BINARY(4) ... to a
            # Drill constant expression".
            raise ProgrammingError(
                'Drill cannot accept binary literals over the REST API; '
                'encode the value (for example as base64 text) first',
                None,
            )
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                raise ProgrammingError(
                    'Drill TIMESTAMP literals are time zone naive; convert '
                    'the datetime to a naive value first',
                    None,
                )
            # Drill requires 'yyyy-MM-dd HH:mm:ss'; the ISO 'T' separator
            # raises DateTimeParseException server side.
            return "'" + value.isoformat(sep=' ') + "'"
        if isinstance(value, time):
            if value.tzinfo is not None:
                raise ProgrammingError(
                    'Drill TIME literals are time zone naive; convert the '
                    'time to a naive value first',
                    None,
                )
            return "'" + value.isoformat() + "'"
        if isinstance(value, date):
            return "'" + value.isoformat() + "'"
        if isinstance(value, Decimal):
            if not value.is_finite():
                raise ProgrammingError(
                    f'Drill has no literal for the decimal value {value}',
                    None,
                )
            return str(value)
        if isinstance(value, Integral):
            return str(int(value))
        if isinstance(value, Real):
            number = float(value)
            if not isfinite(number):
                # str(float("nan")) is "nan", which Drill would parse as a
                # column reference rather than a number.
                raise ProgrammingError(
                    f'Drill has no literal for the float value {number}',
                    None,
                )
            return repr(number)
        raise ProgrammingError(
            f'Unsupported query parameter type: {type(value).__name__}',
            None,
        )

    # One pass over the SQL text, matching whole lexical units so that a
    # question mark inside a string, a quoted identifier or a comment is never
    # mistaken for a placeholder.  Each quoted form uses the doubled-delimiter
    # escape that Drill accepts, and each closing delimiter is optional so an
    # unterminated construct swallows the rest of the statement instead of
    # exposing later text as SQL. Drill 1.21.2 Parser.jj (8581-8612) uses
    # longest-match openers: a formal comment consumes /** AND the following
    # non-slash character before looking for */. Thus /***/ is unterminated,
    # just like /*/. Never reuse an opener character as part of the closer.
    _TOKEN_PATTERN = re.compile(
        r"""
          '[^']*(?:''[^']*)*'?          # string literal
        | "[^"]*(?:""[^"]*)*"?          # double-quoted identifier
        | `[^`]*(?:``[^`]*)*`?          # backtick-quoted identifier
        | (?:--|//)[^\r\n]*(?:\r\n|[\r\n])?  # line comment
        | /\*(?:\*[^/])?[\s\S]*?(?:\*/|\Z)  # block/formal comment
        | \?                            # qmark placeholder
        """,
        re.VERBOSE,
    )

    @classmethod
    def substitute_in_query(cls, string_query, parameters):
        """Substitute qmark parameters without reparsing parameter contents.

        Drill's REST endpoint accepts SQL text rather than a separate parameter
        payload, so the DB-API driver must render literals locally.  Placeholders
        inside SQL strings, quoted identifiers, or comments are not parameters,
        and question marks introduced by a parameter are never visited again.
        """
        logger.info(f'substitutes parameters in query {string_query}.')
        if isinstance(parameters, (dict, str, bytes)):
            raise ProgrammingError(
                'qmark parameters must be supplied as a sequence', None
            )
        if parameters is None:
            parameters = ()

        try:
            parameters = tuple(parameters)
        except TypeError as error:
            raise ProgrammingError(
                'qmark parameters must be supplied as a sequence', None
            ) from error
        output = []
        parameter_index = 0
        position = 0

        for match in cls._TOKEN_PATTERN.finditer(string_query):
            output.append(string_query[position:match.start()])
            token = match.group()
            if token == '?':
                if parameter_index >= len(parameters):
                    raise ProgrammingError(
                        'Not enough query parameters for qmark placeholders',
                        None,
                    )
                literal = cls._sql_literal(parameters[parameter_index])
                logger.debug(f'set parameter value {literal}')
                output.append(literal)
                parameter_index += 1
            else:
                output.append(token)
            position = match.end()

        output.append(string_query[position:])

        if parameter_index != len(parameters):
            raise ProgrammingError(
                'Too many query parameters for qmark placeholders', None
            )
        return ''.join(output)

    def __init__(self, conn):

        self.arraysize: int = 1
        self.description: tuple = None
        self.connection = conn
        self.rowcount: int = -1
        self.rownumber: int = None
        self.result_md = {}

        self._is_open: bool = True
        # Tag of the statement most recently started by execute(); a new
        # tag per statement, so cancel() can only ever reach that statement.
        self.query_tag: str = None
        # Optional caller-assigned ID (32 lowercase hex characters) shared by
        # every statement this cursor runs, e.g. one SQL Lab execution, so a
        # caller that must choose the cancel ID before execution can stop
        # whichever of its statements is running (Connection.cancel_query_group).
        self.cancel_group: str = None
        self._result_event_stream = self._row_stream = None
        self._typecaster_list: list = None

    def is_open(func):
        """Decorator for methods which require a connection"""

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

    def _gen_description(self, col_types, raw_col_types=None):
        blank = [None] * len(self.result_md['columns'])
        dbapi_col_types = [DBAPITypeObject(col_type) for col_type in col_types or ()]
        # Drill sends DECIMAL columns as e.g. "VARDECIMAL(12, 3)".
        precision, scale = list(blank), list(blank)
        for i, raw in enumerate(raw_col_types or ()):
            sized = _DECIMAL_SIZE.fullmatch(str(raw))
            if sized:
                precision[i], scale[i] = int(sized.group(1)), int(sized.group(2))

        self.description = tuple(
            zip(
                self.result_md['columns'],  # name
                dbapi_col_types or blank,  # type_code
                blank,  # display_size
                blank,  # internal_size
                precision,  # precision
                scale,  # scale
                blank   # null_ok
            )
        )

    def _report_query_state(self):
        md = self.result_md
        query_state = md.get('queryState', None)
        exception = md.get(
                    'exception',
                    'No exception returned.'
                )
        error_message = md.get(
            'errorMessage',
            'No error message is returned (which most likely means that ' \
            'drill.exec.http.rest.errors.verbose is set to false.)'
        )
        stack_trace = md.get('stackTrace', 'No stack trace returned.')

        logger.info(
            f'received final query state {query_state}.'
        )

        if query_state != 'COMPLETED':
            logger.warning(exception)
            logger.warning(error_message)
            logger.warning(stack_trace)

            raise DatabaseError(
                f'Final Drill query state is {query_state}. {error_message}',
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
                    # discard the array node itself
                    next(self._result_event_stream)

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

    def close(self):
        # Not @is_open: SQLAlchemy closes cursors after invalidating their
        # connection on a disconnect, and that must still release the stream.
        if self._is_open is False:
            return
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
        self.result_md = {}

        matchObj = re.match(r'^SHOW FILES FROM\s(.+)',
                            operation, re.IGNORECASE)
        if matchObj:
            self._default_storage_plugin = matchObj.group(1)
            logger.info(
                'sets the default storage plugin to '
                f'{self._default_storage_plugin}'
            )

        tag = self.query_tag = uuid4().hex
        group = self.cancel_group
        if group is not None and not re.fullmatch(r'[0-9a-f]{32}', str(group)):
            raise ProgrammingError('cancel_group must be 32 lowercase hex characters', None)
        label = f'{tag} group:{group}' if group else tag
        try:
            resp = self.connection.submit_query(
                f'/* {_QUERY_TAG_PREFIX}{label} */ '
                + self.substitute_in_query(operation, parameters)
            )
        except TransportError as ex:
            if getattr(ex, 'timed_out', False):
                self.connection._cancel_after_timeout(tag)
            raise

        if resp.status_code != 200:
            err_msg = resp.json().get('errorMessage', None)
            raise ProgrammingError(err_msg, resp.status_code)

        on_failure = None
        if self.connection._request_timeout:
            def on_failure(tag=tag):
                self.connection._cancel_after_timeout(tag)
        self._result_event_stream = parse(RequestsStreamWrapper(resp, on_failure))
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
            self._gen_description(basic_coltypes, md)

            self._typecaster_list = [
                self.connection.python_typecasters.get(col, lambda v: v) for
                col in basic_coltypes
            ]
        else:
            self._gen_description(None)
            logger.warning(
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

    def setinputsizes(self, *sizes):
        '''Not supported.'''
        logger.debug('setinputsizes is a no-op in this driver.')

    def setoutputsize(self, size, column=0):
        '''Not supported.'''
        logger.debug('setoutputsize is a no-op in this driver.')

    @is_open
    def get_query_id(self) -> str:
        """Unofficial convenience method for getting the Drill ID of the last query.

        Drill's REST API sends the ID together with the first result batch,
        so it is None until the query has started returning results.
        """
        return self.result_md.get('queryId')

    def cancel(self):
        """Ask Drill to cancel the statement this cursor is running.

        Safe to call from another thread, including while execute() is still
        waiting for the first result batch. Each execute() tags its statement
        with a fresh ID, so only the cursor's most recent statement can be
        cancelled. Returns True iff Drill reports that it cancelled it; False
        if that statement is no longer (or not yet) running.
        """
        if not self.query_tag:
            return False
        return self.connection.cancel_tagged_query(self.query_tag)

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


class Connection:
    def __init__(self,
                 host: str,
                 port: int,
                 proto: str,
                 impersonation_target: str,
                 session: Session,
                 stream_results: bool = True,
                 request_timeout: float = None):
        if session is None:
            raise ProgrammingError('A Requests session is required.', None)

        self._base_url = f'{proto}{host}:{port}'
        self._session = session
        self._connected = True
        self._impersonation_target = impersonation_target
        self._stream_results = stream_results
        # None (the default) keeps the historical behaviour: no client-side
        # limit, so a stalled server blocks the caller indefinitely.
        self._request_timeout = request_timeout

        logger.debug('queries Drill\'s version number...')
        resp = self.submit_query(
            'select min(version) version from sys.drillbits'
        )
        self.drill_version = resp.json()['rows'][0]['version']
        logger.info(f'has connected to Drill version {self.drill_version}.')

        # DOUBLE/FLOAT values arrive as JSON numbers, which the JSON parser
        # yields as Decimal, and NaN/Infinity/-Infinity arrive as strings.
        self.python_typecasters = {
            'FLOAT4': _float_from_json,
            'FLOAT8': _float_from_json,
        }
        if self.drill_version >= '1.19':
            # Starting in 1.19 the Drill REST API returns UNIX times
            self.python_typecasters.update({
                'DATE': DateFromTicks,
                'TIME': TimeFromTicks,
                'TIMESTAMP': TimestampFromTicks
            })
            logger.debug('sets up typecasting functions for Drill >= 1.19.')

    def submit_query(self, query: str, stream: bool = None):
        logger.debug(f'submits a query: {query}')
        payload = api_globals._PAYLOAD.copy()
        payload['userName'] = self._impersonation_target

        # TODO: autoLimit, defaultSchema
        payload['query'] = query

        # Use connection default if not specified
        if stream is None:
            stream = self._stream_results

        logger.debug(f'sends an HTTP POST with payload (stream={stream})')
        logger.debug(payload)

        try:
            resp = self._session.post(
                f'{self._base_url}/query.json',
                data=dumps(payload),
                headers=api_globals._HEADER,
                timeout=self._request_timeout,
                stream=stream
            )
        except Timeout as ex:
            error = TransportError(
                f'Drill REST request timed out after {self._request_timeout} s',
                None
            )
            error.timed_out = True
            raise error from ex
        except RequestException as ex:
            raise _transport_error(ex, 'query request') from ex

        # Never touch resp.text for a streamed response: evaluating it reads
        # the whole body into memory, which silently defeats stream_results.
        if not stream and logger.isEnabledFor(logging.DEBUG):
            logger.debug('received an HTTP response with body:')
            logger.debug(resp.text)

        if resp.status_code == 200:
            return resp

        raise DatabaseError(
            resp.json().get('errorMessage', None),
            resp.status_code
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
            logger.warning(f'encountered {ex} when try to close connection.')
            raise ConnectionClosedException('Failed to close connection') from ex

    @connected
    def cancel_query(self, query_id: str) -> bool:
        """Cancel a running query by ID with Drill's REST cancel endpoint.

        Returns True iff Drill reports that it cancelled the query; False if
        the query is no longer running or Drill could not locate it.
        """
        try:
            resp = self._session.get(
                f'{self._base_url}/profiles/cancel/{quote(query_id, safe="")}',
                timeout=self._request_timeout or 30,
            )
        except Timeout as ex:
            raise TransportError(
                f'Drill REST cancel request timed out for query {query_id}', None
            ) from ex
        except RequestException as ex:
            raise _transport_error(ex, 'cancel request') from ex
        if resp.status_code != 200:
            raise OperationalError(
                f'Drill REST cancel request failed for query {query_id}',
                resp.status_code
            )
        message = resp.text
        logger.info(f'cancel request for {query_id}: {message}')
        return (message.startswith('Cancelled query ')
                or (' canceled on node ' in message
                    and message.startswith('Query ')))

    def _running_tagged(self, attempts: int = 3, want=None):
        """Yield (queryId, tag, group) for running queries carrying a tag.

        Retries briefly (until want() accepts the list) because Drill
        registers a query as running shortly after accepting it.
        """
        entries = []
        for attempt in range(attempts):
            if attempt:
                sleep(0.2)
            try:
                resp = self._session.get(
                    f'{self._base_url}/profiles/running.json',
                    timeout=self._request_timeout or 30,
                )
            except RequestException as ex:
                raise _transport_error(ex, 'running-query lookup') from ex
            if resp.status_code != 200:
                raise OperationalError(
                    'Drill REST running-query lookup failed', resp.status_code)
            entries = []
            for entry in resp.json().get('runningQueries', []):
                match = _QUERY_TAG_RE.match(entry.get('query') or '')
                if match and entry.get('queryId'):
                    entries.append((entry['queryId'], match['tag'], match['group']))
            if want is None or want(entries):
                break
        return entries

    @connected
    def find_tagged_query(self, query_tag: str, attempts: int = 3):
        """Return the ID of the running query carrying query_tag, or None.

        Only an exact, unique tag match is returned.
        """
        def matches(entries):
            return [qid for qid, tag, _ in entries if tag == query_tag]
        found = matches(self._running_tagged(attempts, lambda e: matches(e)))
        if len(found) > 1:
            raise OperationalError(
                f'{len(found)} running queries carry tag {query_tag}', None)
        return found[0] if found else None

    @connected
    def cancel_query_group(self, cancel_group: str, attempts: int = 3) -> bool:
        """Cancel every running statement carrying cancel_group.

        See Cursor.cancel_group. Returns True iff Drill reports that it
        cancelled at least one statement.
        """
        if not re.fullmatch(r'[0-9a-f]{32}', str(cancel_group or '')):
            raise ProgrammingError('cancel_group must be 32 lowercase hex characters', None)
        entries = self._running_tagged(
            attempts, lambda e: any(group == cancel_group for _, _, group in e))
        cancelled = False
        for query_id, _, group in entries:
            if group == cancel_group:
                cancelled = self.cancel_query(query_id) or cancelled
        return cancelled

    def _cancel_after_timeout(self, query_tag: str):
        """Best effort: stop the server query whose request timed out.

        A client-side timeout does not stop Drill, so without this every
        retried timeout would leave another copy of the query running.
        """
        try:
            cancelled = self.cancel_tagged_query(query_tag)
            logger.warning(f'request timed out; server query cancelled: {cancelled}')
        except Exception as ex:  # never mask the original timeout
            logger.warning(f'request timed out; cancelling the server query failed: {ex}')

    @connected
    def cancel_tagged_query(self, query_tag: str) -> bool:
        """Cancel the running query carrying query_tag (see Cursor.query_tag)."""
        query_id = self.find_tagged_query(query_tag)
        return bool(query_id) and self.cancel_query(query_id)

    @connected
    def commit(self):
        logger.debug('commit is a no-op in this driver.')

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
            verify_ssl=True,
            impersonation_target: str = None,
            stream_results: bool = True,
            request_timeout: float = None
            ) -> Connection:
    """
    Establishes a connection with an Apache Drill server.

    This method sets up a connection to an Apache Drill server using the specified
    host, port, and authentication credentials. It supports both secure (SSL/TLS)
    and non-secure connections. If the connection is successful, it returns a
    `Connection` object, which can be used to execute queries and interact with
    the Drill server.

    Parameters:
    host (str): The hostname or IP address of the Apache Drill server.
    port (int, optional): The port number of the Apache Drill server. Defaults to 8047.
    db (str, optional): The initial database/schema to connect to. If not provided, no specific schema is selected.
    use_ssl (bool, optional): Flag indicating whether to use SSL/TLS for the connection. Defaults to False.
    drilluser (str, optional): The username to authenticate with. If not provided, it uses anonymous authentication.
    drillpass (str, optional): The password for the given username. Required if `drilluser` is provided.
    verify_ssl (bool or str, optional): Verify the server certificate for HTTPS connections: True uses the system
                                        trust store, a string is a CA bundle path, False disables verification.
                                        Defaults to True.
    impersonation_target (str, optional): The impersonation target to use for the connection. If provided, operations
                                           will be performed as the specified user.
    stream_results (bool, optional): Flag to enable or disable streaming of query results. Defaults to True.
    request_timeout (float, optional): Seconds to wait for Drill to accept a connection or send the next response
                                       bytes on every REST request. A timeout raises OperationalError. Defaults to
                                       None: no limit, the historical behaviour.

    Returns:
    Connection: An object representing the established connection to the Apache Drill server.

    Raises:
    DatabaseError: If the connection to the Apache Drill server could not be established or an error occurs with the server.
    AuthError: If authentication fails due to invalid username or password.
    """
    if request_timeout is not None:
        try:
            request_timeout = float(request_timeout)
        except (TypeError, ValueError):
            request_timeout = float('nan')
        if not isfinite(request_timeout) or request_timeout <= 0:
            raise ProgrammingError(
                'request_timeout must be a positive number of seconds', None)
    session = Session()

    if verify_ssl is None:
        verify_ssl = True
    if verify_ssl is False and use_ssl in [True, 'True', 'true']:
        logger.warning('TLS certificate verification is disabled (verify_ssl=False).')
    session.verify = verify_ssl
    proto = 'https://' if use_ssl in [True, 'True', 'true'] else 'http://'
    base_url = f'{proto}{host}:{port}'

    logging.info(
        f'will log in with user {drilluser} and impersonation target '
        f'{impersonation_target}'
    )

    if drilluser is None:
        payload = api_globals._PAYLOAD.copy()
        payload['userName'] = impersonation_target

        payload['query'] = 'show schemas'
        login = dict(url=f'{base_url}/query.json', data=dumps(payload),
                     headers=api_globals._HEADER)
    else:
        payload = api_globals._LOGIN.copy()
        payload['j_username'] = drilluser
        payload['j_password'] = drillpass
        login = dict(url=f'{base_url}/j_security_check', data=payload)
    try:
        response = session.post(timeout=request_timeout, **login)
    except SSLError as ex:
        if verify_ssl is not False and 'CERTIFICATE_VERIFY_FAILED' in str(ex):
            # Since 1.1.11.4 certificates are verified by default. Say how to
            # trust a self-signed or private-CA server instead of failing with
            # a bare SSL error; never fall back to an unverified connection.
            raise TransportError(
                f'TLS certificate verification failed for {base_url}: '
                'the server certificate is not trusted by '
                + ('the system trust store' if verify_ssl is True
                   else f'the CA bundle {verify_ssl!r}')
                + '. HTTPS connections verify certificates by default since '
                'sqlalchemy-drill 1.1.11.4. For a self-signed or private-CA '
                'certificate set verify_ssl=<path to the CA bundle that signed '
                'it> in the connection URL. verify_ssl=false disables '
                'verification and is not recommended.',
                None
            ) from ex
        raise _transport_error(ex, 'login request') from ex
    except Timeout as ex:
        raise TransportError(
            f'Drill REST request timed out after {request_timeout} s', None
        ) from ex
    except RequestException as ex:
        raise _transport_error(ex, 'login request') from ex

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

    conn = Connection(host, port, proto, impersonation_target, session,
                      stream_results, request_timeout)
    if db is not None:
        conn.submit_query(f'USE {db}')

    return conn


class RequestsStreamWrapper:
    """
    A file-like view of a streamed Requests response for ijson.

    Reads the body in 64 KiB chunks (the previous implementation consumed
    it one byte at a time) and turns transport failures during streaming
    into DB-API TransportError.
    """

    _CHUNK = 65536

    def __init__(self, resp: Response, on_failure=None):
        self._chunks = resp.iter_content(chunk_size=self._CHUNK)
        self._buffer = bytearray()
        self._on_failure = on_failure

    def read(self, n=-1):
        try:
            while n < 0 or len(self._buffer) < n:
                chunk = next(self._chunks, None)
                if chunk is None:
                    break
                self._buffer += chunk
        except RequestException as ex:
            if self._on_failure is not None:
                self._on_failure()
            raise _transport_error(ex, 'result stream') from ex
        if n < 0:
            n = len(self._buffer)
        data = bytes(self._buffer[:n])
        del self._buffer[:n]
        return data


def _items_once(event_stream, prefix):
    '''
    Generator dispatching native Python objects constructed from the ijson events under the next
    occurrence of the given prefix.  It is similar similar to ijson.items except that it will
    not consume the entire JSON stream looking for occurrences of prefix, but rather stop after
    completing the current occurrence of prefix.  The need for this behaviour is what precluded
    the use of ijson.items instead.
    '''

    try:
        current, event, value = next(event_stream)
    except StopIteration:
        return  # see PEP-479

    while current == prefix:
        if event in ('start_map', 'start_array'):
            object_depth = 1
            builder = ObjectBuilder()
            while object_depth:
                try:
                    builder.event(event, value)
                    current, event, value = next(event_stream)
                    if event in ('start_map', 'start_array'):
                        object_depth += 1
                    elif event in ('end_map', 'end_array'):
                        object_depth -= 1
                except StopIteration:
                    return  # see PEP-479
            del builder.containers[:]
            yield builder.value
        else:
            yield value

        try:
            current, event, value = next(event_stream)
        except StopIteration:
            return  # see PEP-479

    logger.debug(f'finished parsing one occurrence of {prefix}')


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        return -1

    def __eq__(self, other):
        return self.values == other.values

    def __hash__(self):
        return hash(repr(self))


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


_EPOCH = datetime(1970, 1, 1)


def _datetime_from_epoch_ms(ticks):
    # Drill >= 1.19 REST returns DATE, TIME and TIMESTAMP as UTC epoch
    # milliseconds (TIME as milliseconds since midnight). Zero is a real value
    # (1970-01-01, 00:00:00), so only None means NULL. timedelta keeps the
    # millisecond fraction that time.gmtime() would truncate.
    return None if ticks is None else _EPOCH + timedelta(milliseconds=ticks)


_DECIMAL_SIZE = re.compile(
    r'\s*(?:VAR)?DECIMAL\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*', re.IGNORECASE)


def _float_from_json(value):
    """Decode a Drill FLOAT4/FLOAT8 REST value to a Python float.

    Finite values are JSON numbers (parsed as Decimal); Drill writes NaN and
    the infinities as the strings "NaN", "Infinity" and "-Infinity", which
    float() accepts.
    """
    return None if value is None else float(value)


def DateFromTicks(ticks):
    """Construct an object holding a date value from the given Unix time ms."""
    value = _datetime_from_epoch_ms(ticks)
    return None if value is None else value.date()


def TimeFromTicks(ticks):
    """Construct an object holding a time value from the given Unix time ms."""
    value = _datetime_from_epoch_ms(ticks)
    return None if value is None else value.time()


def TimestampFromTicks(ticks):
    """Construct an object holding a timestamp from the given Unix time ms."""
    return _datetime_from_epoch_ms(ticks)


class Binary(bytes):
    """Construct an object capable of holding a binary (long) string value."""
