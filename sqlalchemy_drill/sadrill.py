# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
from sqlalchemy import exc, pool, types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from pandas.core.series import Series

try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler


class DrillIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = compiler.RESERVED_WORDS.copy()
    reserved_words.update(
        [
            'abs', 'all', 'allocate', 'allow', 'alter', 'and', 'any', 'are', 'array', 'as', 'asensitive',
            'asymmetric', 'at', 'atomic', 'authorization', 'avg', 'begin', 'between', 'bigint', 'binary',
            'bit', 'blob', 'boolean', 'both', 'by', 'call', 'called', 'cardinality', 'cascaded', 'case',
            'cast', 'ceil', 'ceiling', 'char', 'character', 'character_length', 'char_length', 'check',
            'clob', 'close', 'coalesce', 'collate', 'collect', 'column', 'commit', 'condition', 'connect',
            'constraint', 'convert', 'corr', 'corresponding', 'count', 'covar_pop', 'covar_samp', 'create',
            'cross', 'cube', 'cume_dist', 'current', 'current_catalog', 'current_date',
            'current_default_transform_group', 'current_path', 'current_role', 'current_schema', 'current_time',
            'current_timestamp', 'current_transform_group_for_type', 'current_user', 'cursor', 'cycle',
            'databases', 'date', 'day', 'deallocate', 'dec', 'decimal', 'declare', 'default', 'default_kw',
            'delete', 'dense_rank', 'deref', 'describe', 'deterministic', 'disallow', 'disconnect', 'distinct',
            'double', 'drop', 'dynamic', 'each', 'element', 'else', 'end', 'end_exec', 'escape', 'every', 'except',
            'exec', 'execute', 'exists', 'exp', 'explain', 'external', 'extract', 'false', 'fetch', 'files', 'filter',
            'first_value', 'float', 'floor', 'for', 'foreign', 'free', 'from', 'full', 'function', 'fusion', 'get',
            'global', 'grant', 'group', 'grouping', 'having', 'hold', 'hour', 'identity', 'if', 'import', 'in',
            'indicator', 'inner', 'inout', 'insensitive', 'insert', 'int', 'integer', 'intersect', 'intersection',
            'interval', 'into', 'is', 'jar', 'join', 'language', 'large', 'last_value', 'lateral', 'leading', 'left',
            'like', 'limit', 'ln', 'local', 'localtime', 'localtimestamp', 'lower', 'match', 'max', 'member', 'merge',
            'method', 'min', 'minute', 'mod', 'modifies', 'module', 'month', 'multiset', 'national', 'natural',
            'nchar', 'nclob', 'new', 'no', 'none', 'normalize', 'not', 'null', 'nullif', 'numeric', 'octet_length',
            'of', 'offset', 'old', 'on', 'only', 'open', 'or', 'order', 'out', 'outer', 'over', 'overlaps', 'overlay',
            'parameter', 'partition', 'percentile_cont', 'percentile_disc', 'percent_rank', 'position', 'power',
            'precision', 'prepare', 'primary', 'procedure', 'range', 'rank', 'reads', 'real', 'recursive', 'ref',
            'references', 'referencing', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2',
            'regr_slope', 'regr_sxx', 'regr_sxy', 'release', 'replace', 'result', 'return', 'returns', 'revoke',
            'right', 'rollback', 'rollup', 'row', 'rows', 'row_number', 'savepoint', 'schemas', 'scope', 'scroll',
            'search', 'second', 'select', 'sensitive', 'session_user', 'set', 'show', 'similar', 'smallint', 'some',
            'specific', 'specifictype', 'sql', 'sqlexception', 'sqlstate', 'sqlwarning', 'sqrt', 'start', 'static',
            'stddev_pop', 'stddev_samp', 'submultiset', 'substring', 'sum', 'symmetric', 'system', 'system_user',
            'table', 'tables', 'tablesample', 'then', 'time', 'timestamp', 'timezone_hour', 'timezone_minute',
            'tinyint', 'to', 'trailing', 'translate', 'translation', 'treat', 'trigger', 'trim', 'true', 'uescape',
            'union', 'unique', 'unknown', 'unnest', 'update', 'upper', 'use', 'user', 'using', 'value', 'values',
            'varbinary', 'varchar', 'varying', 'var_pop', 'var_samp', 'when', 'whenever', 'where', 'width_bucket',
            'window', 'with', 'within', 'without', 'year'
        ]
    )

    def __init__(self, dialect):
        super(DrillIdentifierPreparer, self).__init__(dialect, initial_quote='`', final_quote='`')


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
            try:
                fixed_schema = ""
                if table.schema != "":
                    fixed_schema = ".".join(["`{i}`".format(i=i.replace('`', '')) for i in table.schema.split(".")])
                fixed_table = "{fixed_schema}.`{table_name}`".format(
                    fixed_schema=fixed_schema,table_name=table.name.replace("`", "")
                )
                return fixed_table
            except Exception:
                print("************************************")
                print("Error in DrillCompiler_sadrill.visit_table :: ", Exception.message)
                print("************************************")
        else:
            return ""

    def visit_tablesample(self, tablesample, asfrom=False, **kw):
        print(tablesample)


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

    def create_connect_args(self, url, **kwargs):
        url_port = url.port or 8048
        qargs = {'host': url.host, 'port': url_port}

        try:
            db_parts = (url.database or 'drill').split('/')
            db = ".".join(db_parts)

            # Save this for later use.
            self.host = url.host
            self.port = url_port
            self.username = url.username
            self.password = url.password
            self.db = db

            qargs.update(url.query)
            qargs['db'] = db
            if url.username:
                qargs['drilluser'] = url.username
                qargs['drillpass'] = ""
                if url.password:
                    qargs['drillpass'] = url.password
        except Exception:
            print("************************************")
            print("Error in DrillDialect_sadrill.create_connect_args :: ", Exception.message)
            print("************************************")
        return [], qargs

    def get_selected_workspace(self):
        return self.workspace

    def get_selected_storage_plugin(self):
        return self.storage_plugin

    def has_table(self, connection, table_name, schema=None):
        try:
            self.get_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            print("************************************")
            print("Error in DrillDialect_sadrill.has_table :: ", exc.NoSuchTableError)
            print("************************************")
            return False

    def get_view_names(self, connection, schema=None, **kw):
        return []

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

    def get_schema_names(self, connection, **kw):
        curs = connection.execute("SHOW SCHEMAS")
        result = []
        try:
            for row in curs:
                if row.SCHEMA_NAME != "cp.default" and row.SCHEMA_NAME != "INFORMATION_SCHEMA":
                    result.append(row.SCHEMA_NAME)
        except Exception:
            print("************************************")
            print("Error in DrillDialect_sadrill.get_schema_names :: ", Exception.message)
            print("************************************")

        return tuple(result)

    def get_table_names(self, connection, schema=None, **kw):
        curs = connection.execute("SHOW FILES FROM {0}".format(self.db))
        tables_names = []
        try:
            for row in curs:
                if row.name.find(".view.drill") >= 0:
                    myname = row.name.replace(".view.drill", "")
                else:
                    myname = row.name
                tables_names.append(myname)

        except Exception:
            print("************************************")
            print("Error in DrillDialect_sadrill.get_table_names :: ", Exception.message)
            print("************************************")
        return tuple(tables_names)

    def get_columns(self, connection, table_name, schema=None, **kw):
        result = []
        if "SELECT " in table_name:
            q = "SELECT * FROM ({table_name}) LIMIT 1".format(table_name=table_name)
        else:
            q = "DESCRIBE {table_name}".format(table_name=table_name)

        cursor = connection.execute(q)

        for col in cursor:
            if len(col) > 0:
                cname = col[1].get('Name', "")
                dtype = str(col[1].get('dtype', 'ANY'))
                ctype = _type_map.get(dtype, _type_map['ANY'])
                bisnull = True
                column = {
                    "name": cname,
                    "type": ctype,
                    "default": None,
                    "autoincrement": None,
                    "nullable": bisnull,
                }
                result.append(column)
        return result
