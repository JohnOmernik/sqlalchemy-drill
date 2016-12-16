# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 08:58:12 2016

@author: cgivre
"""
from __future__ import absolute_import
from __future__ import unicode_literals
from distutils.version import StrictVersion
from pydrill.dbapi import drill
from pydrill.client import PyDrill
from sqlalchemy import exc
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy import VARCHAR, INTEGER, FLOAT, DATE, TIMESTAMP, TIME
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
#from .base import DrillCompiler

import re
import sqlalchemy


try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler

class DrillIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(['abs', 'all', 'allocate', 'allow', 'alter', 'and', 'any', 'are', 'array', 'as', 'asensitive', 'asymmetric', 'at', 'atomic', 'authorization', 'avg', 'begin', 'between', 'bigint', 'binary', 'bit', 'blob', 'boolean', 'both', 'by', 'call', 'called', 'cardinality', 'cascaded', 'case', 'cast', 'ceil', 'ceiling', 'char', 'character', 'character_length', 'char_length', 'check', 'clob', 'close', 'coalesce', 'collate', 'collect', 'column', 'commit', 'condition', 'connect', 'constraint', 'convert', 'corr', 'corresponding', 'count', 'covar_pop', 'covar_samp', 'create', 'cross', 'cube', 'cume_dist', 'current', 'current_catalog', 'current_date', 'current_default_transform_group', 'current_path', 'current_role', 'current_schema', 'current_time', 'current_timestamp', 'current_transform_group_for_type', 'current_user', 'cursor', 'cycle', 'databases', 'date', 'day', 'deallocate', 'dec', 'decimal', 'declare', 'default', 'default_kw', 'delete', 'dense_rank', 'deref', 'describe', 'deterministic', 'disallow', 'disconnect', 'distinct', 'double', 'drop', 'dynamic', 'each', 'element', 'else', 'end', 'end_exec', 'escape', 'every', 'except', 'exec', 'execute', 'exists', 'exp', 'explain', 'external', 'extract', 'false', 'fetch', 'files', 'filter', 'first_value', 'float', 'floor', 'for', 'foreign', 'free', 'from', 'full', 'function', 'fusion', 'get', 'global', 'grant', 'group', 'grouping', 'having', 'hold', 'hour', 'identity', 'if', 'import', 'in', 'indicator', 'inner', 'inout', 'insensitive', 'insert', 'int', 'integer', 'intersect', 'intersection', 'interval', 'into', 'is', 'jar', 'join', 'language', 'large', 'last_value', 'lateral', 'leading', 'left', 'like', 'limit', 'ln', 'local', 'localtime', 'localtimestamp', 'lower', 'match', 'max', 'member', 'merge', 'method', 'min', 'minute', 'mod', 'modifies', 'module', 'month', 'multiset', 'national', 'natural', 'nchar', 'nclob', 'new', 'no', 'none', 'normalize', 'not', 'null', 'nullif', 'numeric', 'octet_length', 'of', 'offset', 'old', 'on', 'only', 'open', 'or', 'order', 'out', 'outer', 'over', 'overlaps', 'overlay', 'parameter', 'partition', 'percentile_cont', 'percentile_disc', 'percent_rank', 'position', 'power', 'precision', 'prepare', 'primary', 'procedure', 'range', 'rank', 'reads', 'real', 'recursive', 'ref', 'references', 'referencing', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2', 'regr_slope', 'regr_sxx', 'regr_sxy', 'release', 'replace', 'result', 'return', 'returns', 'revoke', 'right', 'rollback', 'rollup', 'row', 'rows', 'row_number', 'savepoint', 'schemas', 'scope', 'scroll', 'search', 'second', 'select', 'sensitive', 'session_user', 'set', 'show', 'similar', 'smallint', 'some', 'specific', 'specifictype', 'sql', 'sqlexception', 'sqlstate', 'sqlwarning', 'sqrt', 'start', 'static', 'stddev_pop', 'stddev_samp', 'submultiset', 'substring', 'sum', 'symmetric', 'system', 'system_user', 'table', 'tables', 'tablesample', 'then', 'time', 'timestamp', 'timezone_hour', 'timezone_minute', 'tinyint', 'to', 'trailing', 'translate', 'translation', 'treat', 'trigger', 'trim', 'true', 'uescape', 'union', 'unique', 'unknown', 'unnest', 'update', 'upper', 'use', 'user', 'using', 'value', 'values', 'varbinary', 'varchar', 'varying', 'var_pop', 'var_samp', 'when', 'whenever', 'where', 'width_bucket', 'window', 'with', 'within', 'without', 'year'])

    def __init__(self, dialect):
        super(DrillIdentifierPreparer, self). \
            __init__(dialect, initial_quote='`', final_quote='`')


try:
    from sqlalchemy.types import BigInteger
except ImportError:
    from sqlalchemy.databases.mysql import MSBigInteger as BigInteger
_type_map = {
    'bigint': BigInteger,
    'integer': types.Integer,
    'boolean': types.Boolean,
    'double': types.Float,
    'varchar': types.String,
    'timestamp': types.TIMESTAMP,
    'date': types.DATE,
}

class


class DrillCompiler_pydrill(compiler.SQLCompiler):
    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))

    # Strip schema
    def visit_table(self, table, asfrom=False, **kwargs):
        print( "VISIT TABLE..... AAAAAAAHHHHH")
        if asfrom:
            return self.preparer.quote(table.name, '`')
        else:
            return ""

    def visit_fromclause(self, fromclause, **kwargs):
        print( "VICTORY")
        print( fromclause )

    def visit_tablesample(self, tablesample, asfrom=False, **kw):
        print( tablesample)


class DrillDialect_pydrill(default.DefaultDialect):
    name = 'drill'
    driver = 'rest'
    preparer = DrillIdentifierPreparer
    statement_compiler = DrillCompiler_pydrill
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True
    storage_plugin = ""
    workspace = ""

    @classmethod
    def dbapi(cls):
        return drill

    def create_connect_args(self, url):
        db_parts = (url.database or 'drill').split('/')
        kwargs = {
            'host': url.host,
            'port': url.port or 8047,
            'username': url.username,
        }
        kwargs.update(url.query)

        # Save this for later.
        self.host = url.host
        self.port = url.port
        self.username = url.username

        if len(db_parts) == 1:
            kwargs['catalog'] = db_parts[0]
            self.storage_plugin = db_parts[0]
        elif len(db_parts) == 2:
            kwargs['catalog'] = db_parts[0]
            kwargs['schema'] = db_parts[1]
            self.storage_plugin = db_parts[0]
            self.workspace = db_parts[1]

        else:
            raise ValueError("Unexpected database format {}".format(url.database))

        return ([], kwargs)

    def get_schema_names(self, connection, **kw):
        return [row.SCHEMA_NAME for row in connection.execute('SHOW DATABASES')]

    def has_table(self, connection, table_name, schema=None):
        try:
            self._get_table_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            return False


    def get_columns(self, connection, table_name, schema=None, **kw):
        if len(self.workspace) > 0:
            table_name = self.storage_plugin + "." + self.workspace + ".`" + table_name + "`"
        else:
            table_name = self.storage_plugin + ".`" + table_name + "`"

        q = "SELECT * FROM %(table_id)s LIMIT 1" % ({"table_id": table_name})

        # drill = PyDrill(host=self.host, port=self.port)
        #queryResult = drill.query(q)
        columns = connection.execute(q)

        result = []
        for column_name in columns.keys():
            # TODO Handle types better
            column = {
                "name": column_name,
                "type": VARCHAR,
                "default": None,
                "autoincrement": None,
                "nullable": False,
            }

            result.append(column)
        print("RESULT: ")
        print(result)
        return result

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Drill has no support for foreign keys.  Returns an empty list."""
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Drill has no support for primary keys.  Retunrs an empty list."""
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Drill has no support for indexes.  Returns an empty list. """
        return[]

    def get_table_names(self, connection, schema=None, **kw):
        location = ""
        if (len(self.workspace) > 0):
            location = self.storage_plugin + "." + self.workspace
        else:
            location = self.storage_plugin

        drill = PyDrill(host=self.host, port=self.port)
        file_dict = drill.query("SHOW FILES IN " + location)

        temp = []
        for row in file_dict:
            temp.append(row['name'])

        table_names = tuple(temp)

        return table_names

    def do_rollback(self, dbapi_connection):
        # No transactions for Drill
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True

'''
if StrictVersion(sqlalchemy.__version__) < StrictVersion('0.7.0'):
    from pyhive import sqlalchemy_backports

    def reflecttable(self, connection, table, include_columns=None, exclude_columns=None):
        insp = sqlalchemy_backports.Inspector.from_engine(connection)
        return insp.reflecttable(table, include_columns, exclude_columns)
    PrestoDialect.reflecttable = reflecttable
'''

