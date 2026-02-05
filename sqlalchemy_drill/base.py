# This is the MIT license: http://www.opensource.org/licenses/mit-license.php
#
# Copyright (c) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>.
# SQLAlchemy is a trademark of Michael Bayer.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from __future__ import absolute_import
from __future__ import unicode_literals
import logging
from urllib.parse import unquote

from sqlalchemy import exc, pool, types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy import inspect

logger = logging.getLogger('drilldbapi')


_type_map = {
    'bit': types.BOOLEAN,
    'bigint': types.BIGINT,
    'binary': types.LargeBinary,
    'varbinary': types.LargeBinary,
    'boolean': types.BOOLEAN,
    'date': types.DATE,
    'decimal': types.DECIMAL,
    'numeric': types.NUMERIC,
    'double': types.FLOAT,
    'float': types.FLOAT,
    'float4': types.FLOAT,
    'float8': types.FLOAT,
    'real': types.FLOAT,
    'int': types.INTEGER,
    'integer': types.INTEGER,
    'tinyint': types.SMALLINT,
    'smallint': types.SMALLINT,
    'interval': types.Interval,
    'timestamp': types.TIMESTAMP,
    'time': types.TIME,
    'varchar': types.String,
    'char': types.String,
    'character': types.String,
    'character varying': types.String,
    'string': types.String,
    'any': types.String,
    'null': types.NullType,
    'map': types.UserDefinedType,
    'list': types.UserDefinedType,
    'struct': types.UserDefinedType,
    'array': types.UserDefinedType,
    'json': types.JSON,
}


class DrillCompiler_sadrill(compiler.SQLCompiler):

    def default_from(self):
        """Called when a ``SELECT`` statement has no froms,
        and no ``FROM`` clause is to be appended.
       Drill uses FROM values(1)
        """
        return " FROM (values(1))"

    def visit_char_length_func(self, fn, **kw):
        return f'length{self.function_argspec(fn, **kw)}'

    def visit_table(self, table, asfrom=False, **kwargs):
        logger.debug(f"table: {table}")
        if asfrom:
            try:
                fixed_schema = ""
                if table.schema != "":
                    fixed_schema = ".".join(
                        [f"`{i.replace('`', '')}`" for i in table.schema.split(".")])
                fixed_table = f"{fixed_schema}.`{table.name.replace('`', '')}`"
                return fixed_table
            except Exception as ex:
                logger.error(f"Error in DrillCompiler_sadrill.visit_table :: {ex}")
                return ""
        return ""

    def visit_tablesample(self, tablesample, asfrom=False, **kw):
        logger.info(f"{tablesample}")


class DrillTypeCompiler_sadrill(compiler.GenericTypeCompiler):

    def visit_JSON(self, type_, **kwargs):
        # NOTE: might need to do more to fully enable json support
        # see https://gist.github.com/slitayem/c7b87d3f329caaa9794a408ad83ef0e5
        #
        # adding this class+method (plus adding json to `_type_map` above)
        # seems to be enough to avoid exceptions when trying to use tables
        # with json columns like these bugs in other sqlalchemy extensions/dialects:
        # - https://bitbucket.org/estin/sadisplay/issues/17/cannot-render-json-column-type
        #   (fixed by https://bitbucket.org/estin/sadisplay/commits/a49203105e8f4f1048cb28a64f21a2e789f04594)
        # - https://github.com/insightindustry/sqlathanor/issues/63
        #   (fixed by https://github.com/insightindustry/sqlathanor/commit/697bd455d4c38aa8a0888e118106dea429f06f9e)
        # - https://github.com/sqlalchemy-bot/test_sqlalchemy/issues/3549
        # - https://stackoverflow.com/questions/13484900/generate-sql-string-using-schema-createtable-fails-with-postgresql-array
        return 'JSON'


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
            'precision', 'prepare', 'primary', 'procedure', 'properties', 'range', 'rank', 'reads', 'real', 'recursive',
            'ref', 'references', 'referencing', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2',
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
        super().__init__(dialect, initial_quote='`', final_quote='`')

    def format_drill_table(self, schema, isFile=True):
        formatted_schema = ""

        num_dots = schema.count(".")
        schema = schema.replace('`', '')

        # For a file, the last section will be the file extension
        schema_parts = schema.split('.')

        if isFile and num_dots == 3:
            # Case for File + Workspace
            plugin = schema_parts[0]
            workspace = schema_parts[1]
            table = schema_parts[2] + "." + schema_parts[3]
            formatted_schema = plugin + ".`" + workspace + "`.`" + table + "`"
        elif isFile and num_dots == 2:
            # Case for file and no workspace
            plugin = schema_parts[0]
            formatted_schema = plugin + "." + \
                schema_parts[1] + ".`" + schema_parts[2] + "`"
        else:
            # Case for non-file plugins or incomplete schema parts
            for part in schema_parts:
                quoted_part = "`" + part + "`"
                if len(formatted_schema) > 0:
                    formatted_schema += "." + quoted_part
                else:
                    formatted_schema = quoted_part

        return formatted_schema


class DrillDialect(default.DefaultDialect):
    name = 'drilldbapi'
    driver = 'rest'
    preparer = DrillIdentifierPreparer
    statement_compiler = DrillCompiler_sadrill
    type_compiler = DrillTypeCompiler_sadrill
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
        super().__init__(**kw)
        self.supported_extensions = []
        # Initialize attributes that will be set in create_connect_args
        self.host = None
        self.port = None
        self.username = None
        self.password = None
        self.db = None
        self.storage_plugin = None
        self.workspace = None
        self.plugin_type = None
        self.quoted_schema = None

    @classmethod
    def import_dbapi(cls):
        import sqlalchemy_drill.drilldbapi as module  # pylint: disable=import-outside-toplevel
        return module

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()

    def create_connect_args(self, url, **kwargs):
        url_port = url.port or 8047
        qargs = {'host': url.host, 'port': url_port}

        try:
            # URL-decode the database path to handle encoded characters like %2F -> /
            raw_database = unquote(url.database) if url.database else 'drill'
            db_parts = raw_database.split('/')
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

            # Convert stream_results to boolean if present
            if 'stream_results' in qargs:
                qargs['stream_results'] = qargs['stream_results'] in [True, 'True', 'true', '1']

            if url.username:
                qargs['drilluser'] = url.username
                qargs['drillpass'] = ""
                if url.password:
                    qargs['drillpass'] = url.password
        except Exception as ex:
            logger.error(f"Error in DrillDialect_sadrill.create_connect_args :: {ex}")

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
                if row.SCHEMA_NAME not in ('cp.default', 'INFORMATION_SCHEMA', 'dfs.default'):
                    result.append(row.SCHEMA_NAME)
        except Exception as ex:
            logger.error(f"Error in DrillDialect_sadrill.get_schema_names :: {ex}")

        return tuple(result)

    def get_selected_workspace(self):
        logger.info(f"Selected Workspace: {self.workspace}")
        return self.workspace

    def get_selected_storage_plugin(self):
        logger.info(f"Storage Plugin: {self.storage_plugin}")
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
                        # Exclude views
                        continue
                    myname = row.name
                    tables_names.append(myname)

            except Exception as ex:
                logger.error(f"Error in DrillDialect_sadrill.get_table_names :: {ex}")

            return tuple(tables_names)

        curs = connection.execute(
            f"SELECT `TABLE_NAME` AS name FROM INFORMATION_SCHEMA.`TABLES` WHERE `TABLE_SCHEMA` = '{schema}'")
        tables_names = []
        try:
            for row in curs:
                if row.name.find(".view.drill") >= 0:
                    myname = row.name.replace(".view.drill", "")
                else:
                    myname = row.name
                tables_names.append(myname)

        except Exception as ex:
            logger.error(f"Error in DrillDialect_sadrill.get_table_names :: {ex}")

        return tuple(tables_names)

    def get_view_names(self, connection, schema=None, **kw):
        view_names = []
        curs = connection.execute(
            f"SELECT `TABLE_NAME` FROM INFORMATION_SCHEMA.views WHERE table_schema='{schema}'")
        try:
            for row in curs:
                myname = row.TABLE_NAME
                view_names.append(myname)

        except Exception as ex:
            logger.error(f"Error in DrillDialect_sadrill.get_view_names :: {ex}")

        return tuple(view_names)

    def has_table(self, connection, table_name, schema=None, **kwargs) -> bool:
        try:
            self.get_columns(connection, table_name, schema)
            return True
        except exc.NoSuchTableError as e:
            logger.error(f"Error in DrillDialect_sadrill.has_table :: {e}")
            return False

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True

    @staticmethod
    def object_as_dict(obj):
        return {c.key: getattr(obj, c.key)
                for c in inspect(obj).mapper.column_attrs}

    def get_data_type(self, data_type):
        logger.debug(f"Drill data type: {data_type}")
        try:
            return _type_map[data_type]
        except KeyError:
            logger.warning(f"Unknown Drill data type: '{data_type}', using UserDefinedType")
            return types.UserDefinedType

    def get_columns(self, connection, table_name, schema=None, **kw):
        result = []

        plugin_type = self.get_plugin_type(connection, schema)

        # Plugins with dynamic schemas use ** notation - query data directly
        if plugin_type in ('file', 'mongo', 'splunk'):
            views = self.get_view_names(connection, schema)

            file_name = schema + "." + table_name
            quoted_file_name = self.identifier_preparer.format_drill_table(
                file_name, isFile=True)

            # MongoDB and Splunk use ** notation - query data directly to get schema
            if plugin_type == "mongo":
                mongo_quoted_file_name = self.identifier_preparer.format_drill_table(
                    file_name, isFile=False)
                q = f"SELECT `**` FROM {mongo_quoted_file_name} LIMIT 1"
            elif table_name in views:
                logger.debug(f"View: {quoted_file_name}, {table_name}, {schema}")
                view_name = f"`{schema}`.`{table_name}`"
                q = f"SELECT * FROM {view_name} LIMIT 1"
            else:
                q = f"SELECT * FROM {quoted_file_name} LIMIT 1"

            column_metadata = connection.execute(q).cursor.description

            for row in column_metadata:
                # row[1] is a DBAPITypeObject - extract the type name from its values
                type_obj = row[1]
                if hasattr(type_obj, 'values') and type_obj.values:
                    data_type = type_obj.values[0].lower()
                else:
                    data_type = str(type_obj).lower()
                # Strip precision info like varchar(100) or decimal(10, 2)
                if '(' in data_type:
                    data_type = data_type.split('(')[0]
                logger.debug(f"Getting data type: {data_type}")
                drill_data_type = self.get_data_type(data_type)
                column = {
                    "name": row[0],
                    "type": drill_data_type,
                    "longtype": drill_data_type
                }
                result.append(column)
            logger.debug(f"GET COLUMN QUERY RESULTS: {result}")
            return result

        if "SELECT " in table_name:
            q = f"SELECT * FROM ({table_name}) LIMIT 1"
        else:
            quoted_schema = self.identifier_preparer.format_drill_table(
                schema + "." + table_name, isFile=False)
            q = f"DESCRIBE {quoted_schema}"
        logger.debug(f"QUERY: {q}")
        query_results = connection.execute(q)

        for row in query_results:
            logger.debug(f"Getting (1) data type: {row[1].lower()}")

            column = {
                "name": row[0],
                "type": self.get_data_type(str(row[1]).lower()),
                "longType": self.get_data_type(str(row[1]).lower())
            }
            result.append(column)
        logger.debug(f"Result: {result}")
        return result

    def get_plugin_type(self, connection, plugin=None):
        if plugin is None:
            return None

        try:
            query = f"""SELECT SCHEMA_NAME, TYPE
            FROM INFORMATION_SCHEMA.`SCHEMATA`
            WHERE SCHEMA_NAME LIKE '%{plugin.replace('`', '')}%'"""

            rows = connection.execute(query).fetchall()
            plugin_type = ""
            for row in rows:
                plugin_type = row[1]
                # plugin_name = row[0]  # unused

            return plugin_type

        except Exception as ex:
            logger.error(f"Error in DrillDialect_sadrill.get_plugin_type :: {ex}")
            return None
