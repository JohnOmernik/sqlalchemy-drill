# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
from sqlalchemy import exc, pool, types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy import inspect
import pathlib
import requests
from pprint import pprint

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

    supported_file_extensions = [".csv",".txt",".avro",".json",".parquet",".tsv",".psv"]

    def __init__(self, dialect):
        super(DrillIdentifierPreparer, self).__init__(dialect, initial_quote='`', final_quote='`')

    def format_drill_table(self, schema, isFile=True):

        schema = schema.replace('`', '')

        schema_parts = schema.split('.')

        if isFile:

            extension = pathlib.Path(schema).suffix
            
            if not extension.lower() in self.supported_file_extensions:
                print("file system based schema encountered (perhaps a query on a directory/table ? ) ")
                return ".".join(["`" + x + "`" for x in schema_parts])
            else:
                print("file extension exists in supported types ")
                plugin = schema_parts[0]
                workspace = schema_parts[1]
                table = ".".join(schema_parts[2:])
                return "`" + plugin + "`" + ".`" + workspace + "`.`" + table + "`"

        else:
            print("not a file based schema ")
            return ".".join(["`" + part + "`" for part in schema_parts])


try:
    from sqlalchemy.types import BigInteger
except ImportError:
    from sqlalchemy.databases.mysql import MSBigInteger as BigInteger

_type_map = {
    'bit': types.BOOLEAN,
    'BIT': types.BOOLEAN,
    'bigint': types.BIGINT,
    'BIGINT': types.BIGINT,
    'binary': types.LargeBinary,
    'BINARY': types.LargeBinary,
    'boolean': types.BOOLEAN,
    'BOOLEAN': types.BOOLEAN,
    'date': types.DATE,
    'DATE': types.DATE,
    'decimal': types.DECIMAL,
    'DECIMAL': types.DECIMAL,
    'double': types.FLOAT,
    'DOUBLE': types.FLOAT,
    'int': types.INTEGER,
    'INT': types.INTEGER,
    'integer': types.INTEGER,
    'INTEGER': types.INTEGER,
    'interval': types.Interval,
    'INTERVAL': types.Interval,
    'smallint': types.SMALLINT,
    'SMALLINT': types.SMALLINT,
    'timestamp': types.TIMESTAMP,
    'TIMESTAMP': types.TIMESTAMP,
    'time': types.TIME,
    'TIME': types.TIME,
    'varchar': types.String,
    'VARCHAR': types.String,
    'character varying': types.String,
    'CHARACTER VARYING': types.String,
    'ANY': types.String,
    'any': types.String,
    'map': types.UserDefinedType,
    'MAP': types.UserDefinedType
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
            except Exception as ex:
                print("************************************")
                print("Error in DrillCompiler_sadrill.visit_table :: ", str(ex))
                print("************************************")
        else:
            return ""

    def visit_tablesample(self, tablesample, asfrom=False, **kw):
        print(tablesample)

class DrillDialect_sadrill(default.DefaultDialect):

    name = 'drilldbapi'
    driver = 'rest'
    dbapi = ""
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

    def __init__(self, **kw):
        default.DefaultDialect.__init__(self, **kw)
        self.supported_extensions = []

    @classmethod
    def dbapi(cls):
        import sqlalchemy_drill.drilldbapi as module
        return module

    def create_connect_args(self, url, **kwargs):
        url_port = url.port or 8047
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


            # Get Storage Plugin Info:
            if db_parts[0]:
                self.storage_plugin = db_parts[0]

            if len(db_parts) > 1:
                self.workspace = db_parts[1]

            qargs.update(url.query)
            qargs['db'] = db
            if url.username:
                qargs['drilluser'] = url.username
                qargs['drillpass'] = ""
                if url.password:
                    qargs['drillpass'] = url.password
        except Exception as ex:
            print("************************************")
            print("Error in DrillDialect_sadrill.create_connect_args :: ", str(ex))
            print("************************************")
        return [], qargs

    def do_rollback(self, dbapi_connection):
        # No transactions for Drill
        pass

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Drill has no support for foreign keys.  Returns an empty list."""
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        """Drill has no support for indexes.  Returns an empty list. """
        return []

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Drill has no support for primary keys.  Retunrs an empty list."""
        return []

    def get_schema_names(self, connection, **kw):

        # Get table information
        query = "SHOW DATABASES"

        curs = connection.execute(query)
        result = []
        try:
            for row in curs:
                if row.SCHEMA_NAME != "cp.default" and row.SCHEMA_NAME != "INFORMATION_SCHEMA":
                    result.append(row.SCHEMA_NAME)
        except Exception as ex:
            print("************************************")
            print("Error in DrillDialect_sadrill.get_schema_names :: ", str(ex))
            print("************************************")

        return tuple(result)

    def get_selected_workspace(self):
        return self.workspace

    def get_selected_storage_plugin(self):
        return self.storage_plugin

    def get_table_names(self, connection, schema=None, **kw):
        if schema is None:
            schema = connection.engine.url.database
        # Clean up schema

        quoted_schema = self.identifier_preparer.format_drill_table(schema)
        quoted_schema = quoted_schema.replace("/", ".")

        # https://docs.sqlalchemy.org/en/latest/core/connections.html#translation-of-schema-names
        plugin_type = self.get_plugin_type(connection, quoted_schema)

        self.plugin_type = plugin_type
        self.quoted_schema = quoted_schema

        if plugin_type == 'file':
            curs = connection.execute("SHOW FILES FROM " + quoted_schema)
            tables_names = []
            try:
                for row in curs:
                    if row.name.find(".view.drill") >= 0:
                        myname = row.name.replace(".view.drill", "")
                    else:
                        myname = row.name
                    tables_names.append(myname)

            except Exception as ex:
                print("************************************")
                print("Error in DrillDialect_sadrill.get_table_names :: ", str(ex))
                print("************************************")
            return tuple(tables_names)
        else:
            curs = connection.execute(
                "SELECT `TABLE_NAME` AS name FROM INFORMATION_SCHEMA.`TABLES` WHERE `TABLE_SCHEMA` = '" + schema + "'")
            tables_names = []
            try:
                for row in curs:
                    if row.name.find(".view.drill") >= 0:
                        myname = row.name.replace(".view.drill", "")
                    else:
                        myname = row.name
                    tables_names.append(myname)

            except Exception as ex:
                print("************************************")
                print("Error in DrillDialect_sadrill.get_table_names :: ", str(ex))
                print("************************************")
            return tuple(tables_names)

    def get_view_names(self, connection, schema=None, **kw):
        return []

    def has_table(self, connection, table_name, schema=None):
        try:
            self.get_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError:
            print("************************************")
            print("Error in DrillDialect_sadrill.has_table :: ", exc.NoSuchTableError)
            print("************************************")
            return False

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True

    def object_as_dict(obj):
        return {c.key: getattr(obj, c.key)
                for c in inspect(obj).mapper.column_attrs}

    def get_columns(self, connection, table_name, schema=None, **kw):

        result = []

        plugin_type = self.get_plugin_type(connection, schema)

        if plugin_type == "file":
            file_name = schema + "." + table_name
            quoted_file_name = self.identifier_preparer.format_drill_table(file_name, isFile=True)
            q = "SELECT * FROM {file_name} LIMIT 1".format(file_name=quoted_file_name)
            column_metadata = connection.execute(q).cursor.description

            for row in column_metadata:
                column = {
                    "name": row[0],
                    "type": _type_map[row[1].lower()],
                    "longtype": _type_map[row[1].lower()]
                }
                result.append(column)
            return result

        elif "SELECT " in table_name:
            q = "SELECT * FROM ({table_name}) LIMIT 1".format(table_name=table_name)
        else:
            quoted_schema  = self.identifier_preparer.format_drill_table(schema + "." + table_name, isFile=False)
            q = "DESCRIBE {table_name}".format(table_name=quoted_schema)

        query_results = connection.execute(q)

        for row in query_results:
            column = {
                "name": row[0],
                "type": _type_map[row[1].lower()],
                "longType": _type_map[row[1].lower()]
            }
            result.append(column)
        return result

    def get_plugin_type(self, connection, plugin=None):
        if plugin is None:
            return

        try:
            query = "SELECT SCHEMA_NAME, TYPE FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE SCHEMA_NAME LIKE '%" + plugin.replace('`','') + "%'"

            rows = connection.execute(query).fetchall()
            plugin_type = ""
            for row in rows:
                plugin_type = row[1]
                plugin_name = row[0]

            return plugin_type

        except Exception as ex:
                print("************************************")
                print("Error in DrillDialect_sadrill.get_plugin_type :: ", str(ex))
                print("************************************")
                return False
