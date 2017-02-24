# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 08:58:12 2016

@author: cgivre
"""
from __future__ import absolute_import
from __future__ import unicode_literals
from distutils.version import StrictVersion
from sqlalchemy_drill.drilldbapi import drill
from pydrill.client import PyDrill
from sqlalchemy import exc, pool, types
from sqlalchemy import util
from sqlalchemy import VARCHAR, INTEGER, FLOAT, DATE, TIMESTAMP, TIME, Interval, DECIMAL, LargeBinary, BIGINT, SMALLINT
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
    'bigint': types.BIGINT,
    'binary': types.LargeBinary,
    'boolean': types.BOOLEAN,
    'date': types.DATE,
    'decimal': types.DECIMAL,
    'double': types.FLOAT,
    'integer': types.INTEGER,
    'interval': types.Interval,
    'smallint': types.SMALLINT,
    'timestamp': types.TIMESTAMP,
    'time': types.TIME,
    'varchar': types.String
}


class DrillCompiler_sadrill(compiler.SQLCompiler):
    def default_from(self):
        """Called when a ``SELECT`` statement has no froms,
        and no ``FROM`` clause is to be appended.
       Drill uses FROM values(1)
        """
        return " FROM (values(1))"

    def visit_char_length_func(self, fn, **kw):
        return 'length{}'.format(self.function_argspec(fn, **kw))

    def visit_table(self, table, asfrom=False, **kwargs):
        if asfrom:
            storage_plugin = self.dialect.storage_plugin
            workspace = self.dialect.workspace

            full_table = storage_plugin + "." + workspace + "."
            if not table.name.startswith( full_table ):
                corrected_table = full_table + self.preparer.quote(table.name, '`')
                print( "Fixed table: " + corrected_table)
                return corrected_table
            else:
                return self.preparer.quote(table.name, '`')
        else:
            return ""


    def visit_tablesample(self, tablesample, asfrom=False, **kw):
        print( tablesample)



class DrillDialect_sadrill(default.DefaultDialect):
    name = 'drill'
    driver = 'rest'
    preparer = DrillIdentifierPreparer
    statement_compiler = DrillCompiler_sadrill
    poolclass = pool.SingletonThreadPool
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
        print("########### in sadrill.dbapi")
        return drill
    def connect(self, *cargs, **cparams):
        print("############ In sadrill.DrillDialect.connect")
        print(cargs)
        print(cparams)
        return self.dbapi.PyDrill(autocommit=True, *cargs, **cparams)

    def create_connect_args(self, url):

        db_parts = (url.database or 'drill').split('/')

        if url.username:
            if url.password:
                p = url.password
            else:
                p = ""
    
            drill_auth = url.username + ":" + p
        else:
            drill_auth = ""
        kwargs = {
            'host': url.host,
            'port': url.port or 8047,
            'drill_auth':  drill_auth,
 #           'username': url.username,
        }
        kwargs.update(url.query)

        # Save this for later.
        self.host = url.host
        self.port = url.port
        self.drill_auth = drill_auth
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


        print(kwargs)
        print(url)
        return ([], kwargs)

    def get_schema_names(self, connection, **kw):
        return [row.SCHEMA_NAME for row in connection.execute('SHOW DATABASES')]

    def get_selected_workspace(self):
        return self.workspace

    def get_selected_storage_plugin(self):
        return self.storage_plugin

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


        columns = connection.execute(q)
        result = []
        db = drill.connect(host=self.host, port=self.port)
        cursor = db.cursor()
        cursor.execute(q)
        for info in cursor.description:
            print( "ROW INFO!!")
            print( info )
            try:
                coltype = _type_map[info[1]]
            except KeyError:
                #If the type is unknown, make it a VARCHAR
                coltype = types.VARCHAR

            column = {
                "name": info[0],
                "type": coltype,
                "default": None,
                "autoincrement": None,
                "nullable": True,
            }
            print( column )
            result.append(column)

        return result


    def get_table_names(self, connection, schema=None, **kw):
        location = ""
        if (len(self.workspace) > 0):
            location = self.storage_plugin + "." + self.workspace
        else:
            location = self.storage_plugin
        
        print("******in get table names")
        print(connection)
        print(self)
        print (kw)

        drill = PyDrill(host=self.host, port=self.port)
        file_dict = drill.query("SHOW FILES IN " + location)

        temp = []
        for row in file_dict:
            temp.append(row['name'])

        table_names = tuple(temp)

        return table_names

    def get_view_names(self, connection, schema=None, **kw):
        location = ""
        if (len(self.workspace) > 0):
            location = self.storage_plugin + "." + self.workspace
        else:
            location = self.storage_plugin

        drill = PyDrill(host=self.host, port=self.port)
        file_dict = drill.query("SHOW TABLES IN " + location)

        temp = []
        for row in file_dict:
            temp.append(row['name'])

        table_names = tuple(temp)

        return table_names

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Drill has no support for foreign keys.  Returns an empty list."""
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Drill has no support for primary keys.  Retunrs an empty list."""
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Drill has no support for indexes.  Returns an empty list. """
        return[]




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

