# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 08:58:12 2016

@author: cgivre
"""
from __future__ import absolute_import
from __future__ import unicode_literals
from distutils.version import StrictVersion
#from sqlalchemy_drill.drilldbapi import drill
#from pydrill.client import PyDrill
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
    'BIGINT': types.BIGINT,
    'binary': types.LargeBinary,
    'boolean': types.BOOLEAN,
    'date': types.DATE,
    'DATE': types.DATE,
    'decimal': types.DECIMAL,
    'double': types.FLOAT,
    'integer': types.INTEGER,
    'interval': types.Interval,
    'smallint': types.SMALLINT,
    'timestamp': types.TIMESTAMP,
    'TIMESTAMP': types.TIMESTAMP,
    'time': types.TIME,
    'varchar': types.String,
    'CHARACTER VARYING': types.String,
    'ANY': types.String
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
            if table.schema != "":
                fixed_schema = ".".join(["`" + i.replace('`', '') + "`" for i in table.schema.split(".")])
                fixed_table = fixed_schema + ".`" + table.name.replace("`", "") + "`"
            else:
                fixed_table = "`" + table.name.replace("`", "") + "`"
            return fixed_table
        else:
            return ""


    def visit_tablesample(self, tablesample, asfrom=False, **kw):
        print( tablesample)



class DrillDialect_sadrill(default.DefaultDialect):
    name = 'drilldbapi'
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
        import sqlalchemy_drill.drilldbapi as module
        return module

    def connect(self, *cargs, **cparams):
        return self.dbapi.connect(*cargs, **cparams)

    def create_connect_args(self, url, **kwargs):
        db_parts = (url.database or 'drill').split('/')
        db = ".".join(db_parts)
        if url.username:
            if url.password:
                p = url.password
            else:
                p = ""
            qargs = {
                'host': url.host,
                'port': url.port or 8048,
                'drilluser':  url.username,
                'drillpass': p
             }

        else:
            qargs = {
                'host': url.host,
                'port': url.port
             }

        qargs.update(url.query)

        # Save this for later.
        self.host = url.host
        self.port = url.port
        self.username = url.username
        self.password = url.password
        qargs['db'] = db
        self.db = db

        return ([], qargs)


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

    def LIMIT1_get_columns(self, connection, table_name, schema=None, **kw):
        # This method stinks at getting columns from a speed perspective, because it's slow (it has to process a query)
        # However, it's accurate, because it takes a look at the data, and provides a good representation of the types it saw. (for 1 record, not idea...)

        q = "SELECT * FROM %(table_id)s LIMIT 1" % ({"table_id": table_name})#

#        q = "DESCRIBE %(table_id)s" % ({"table_id": table_name})
        cursor = connection.execute(q)


        desc = cursor.cursor.getdesc()
        result = []
        for col in desc:
            cname = col[0]
            bisnull = True
            ctype = _type_map[col[1]]
            column = {
                "name": cname,
                "type": ctype,
                "default": None,
                "autoincrement": None,
                "nullable": bisnull,
            }
            result.append(column)
        return(result)

    def get_columns(self, connection, table_name, schema=None, **kw):


        q = "DESCRIBE %(table_id)s" % ({"table_id": table_name})
        cursor = connection.execute(q)

        result = []
        for col in cursor:
            cname = col[0]
            ctype = _type_map[col[1]]
            bisnull = True
            column = {
                "name": cname,
                "type": ctype,
                "default": None,
                "autoincrement": None,
                "nullable": bisnull,
            }
            result.append(column)
        return(result)


    def get_table_names(self, connection, schema=None, **kw):
        curs = connection.execute("SHOW FILES FROM {0}".format(self.db))
        temp = []
        for row in curs:
            if row.name.find(".view.drill") >= 0:
                myname = row.name.replace(".view.drill", "")
            else:
                myname = row.name
            temp.append(myname)
        table_names = tuple(temp)
        return table_names

    def get_schema_names(self, connection, **kw):
        curs = connection.execute("SHOW SCHEMAS")
        result = []

        for row in curs:
            if row.SCHEMA_NAME != "cp.default" and row.SCHEMA_NAME != "INFORMATION_SCHEMA":
                result.append(row.SCHEMA_NAME)
        return tuple(result)
#        return [row.SCHEMA_NAME for row in connection.execute('SHOW SCHEMAS')]
    def get_view_names(self, connection, schema=None, **kw):
        return []
 #       curs = connection.execute("SHOW FILES")
 #       temp = []
 #       for row in curs:
 #           print(row.name)
 #           print(row.isFile)
 #           if row.name.find(".view.drill") >= 0 and row.isFile == "true":
 #               myname = row.name.replace(".view.drill", "")
 #               temp.append(myname)
 #       view_names = tuple(temp)
 #       return view_names

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

