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

from sqlalchemy import exc, inspect, pool, text, types
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import compiler
from sqlalchemy.sql.elements import quoted_name

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

    def visit_column(self, column, add_to_result_map=None, include_table=True,
                     **kw):
        """Render a column reference without a schema qualifier.

        SQLAlchemy prefixes a column with ``schema.table.``, but Drill only
        accepts a one-part table qualifier: ``SELECT cp.`employee.json`.x FROM
        cp.`employee.json``` fails validation while ``SELECT `employee.json`.x
        FROM cp.`employee.json``` succeeds.  Strip exactly the prefix
        SQLAlchemy added, leaving the table qualifier (needed to disambiguate
        joins) intact.
        """
        text = super().visit_column(
            column,
            add_to_result_map=add_to_result_map,
            include_table=include_table,
            **kw
        )
        table = column.table
        if not include_table or table is None:
            return text

        schema = self.preparer.schema_for_object(table)
        if not schema:
            return text

        schema_prefix = f'{self.preparer.quote_schema(schema)}.'
        if text.startswith(schema_prefix):
            return text[len(schema_prefix):]
        return text

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
        super().__init__(
            dialect,
            initial_quote='`',
            final_quote='`',
            escape_quote='`',
        )

    @staticmethod
    def _schema_parts(schema):
        """Return Drill's plugin/workspace path as distinct identifiers."""
        if schema is None or str(schema) == "":
            return ()

        # Translation tokens carry the *source* schema as an opaque map key.
        # In particular, rewriting a slash here changes which schema is selected.
        if (isinstance(schema, quoted_name) and schema.quote is False
                and schema.startswith("__[SCHEMA_") and schema.endswith("]")):
            return (schema,)

        # SQLAlchemy URLs commonly spell ``dfs.tmp`` as ``dfs/tmp``.  Dots and
        # slashes in a schema therefore delimit Drill's plugin/workspace path;
        # table names are always passed separately so file extensions remain a
        # single identifier.
        parts = str(schema).replace("/", ".").split(".")
        if any(part == "" for part in parts):
            raise ValueError("Drill schema paths cannot contain empty components")
        # SQLAlchemy schema-translation tokens are quoted_name(quote=False).
        # Keep this contract until the compiler substitutes the actual schema;
        # otherwise a mapped plugin.workspace becomes one backticked token.
        quote = getattr(schema, "quote", None)
        return tuple(quoted_name(part, quote=quote) for part in parts)

    def quote_schema(self, schema, force=None):
        """Quote each component of a qualified Drill schema independently."""
        # ``force`` has had no effect in SQLAlchemy since 0.9 and is absent
        # from the current 2.1 signature.  Accept it only for call compatibility.
        return ".".join(self.quote(part) for part in self._schema_parts(schema))

    def format_drill_schema(self, schema):
        """Format a plugin/workspace path for an identifier-only SQL clause."""
        return self.quote_schema(schema)

    def format_drill_table(self, schema, table_name):
        """Format a Drill table without confusing file extensions for schemas.

        Schema and table must be passed separately.  The old single-string
        ``format_drill_table(path, isFile=...)`` signature is gone rather than
        shimmed: silently accepting it produced wrong identifiers such as
        ``dfs.tmp.f.csv.`False``` instead of raising.  Use
        :meth:`format_drill_schema` to format a schema on its own.
        """
        if not isinstance(table_name, str):
            raise TypeError(
                "format_drill_table() requires a string table name; the "
                "legacy isFile argument is no longer supported"
            )

        schema_name = self.format_drill_schema(schema)
        quoted_table = self.quote(table_name)
        if schema_name:
            return f"{schema_name}.{quoted_table}"
        return quoted_table


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
    supports_statement_cache = True

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

    @staticmethod
    def _schema_name(connection, schema):
        """Resolve a reflection schema and normalize Drill URL path syntax."""
        if schema is None:
            schema = connection.engine.url.database
        if schema is None:
            return None
        return str(schema).replace("/", ".")

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        curs = connection.execute(text("SHOW DATABASES"))
        return tuple(
            row[0]
            for row in curs
            if row[0] not in ('cp.default', 'INFORMATION_SCHEMA', 'dfs.default')
        )

    def get_selected_workspace(self):
        logger.info(f"Selected Workspace: {self.workspace}")
        return self.workspace

    def get_selected_storage_plugin(self):
        logger.info(f"Storage Plugin: {self.storage_plugin}")
        return self.storage_plugin

    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        schema = self._schema_name(connection, schema)
        plugin_type = self.get_plugin_type(
            connection, schema, info_cache=kw.get("info_cache"))

        if plugin_type == 'file':
            quoted_schema = self.identifier_preparer.format_drill_schema(schema)
            curs = connection.exec_driver_sql(f"SHOW FILES FROM {quoted_schema}")
            return tuple(
                row[0]
                for row in curs
                if ".view.drill" not in row[0]
            )

        curs = connection.execute(
            text(
                "SELECT `TABLE_NAME` AS name "
                "FROM INFORMATION_SCHEMA.`TABLES` "
                "WHERE `TABLE_SCHEMA` = :schema"
            ),
            {"schema": schema},
        )
        return tuple(
            row[0].replace(".view.drill", "")
            if ".view.drill" in row[0] else row[0]
            for row in curs
        )

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        schema = self._schema_name(connection, schema)
        curs = connection.execute(
            text(
                "SELECT `TABLE_NAME` "
                "FROM INFORMATION_SCHEMA.`VIEWS` "
                "WHERE `TABLE_SCHEMA` = :schema"
            ),
            {"schema": schema},
        )
        return tuple(row[0] for row in curs)

    @reflection.cache
    def has_table(self, connection, table_name, schema=None, **kwargs):
        schema = self._schema_name(connection, schema)
        curs = connection.execute(
            text(
                "SELECT 1 FROM INFORMATION_SCHEMA.`TABLES` "
                "WHERE `TABLE_SCHEMA` = :schema "
                "AND `TABLE_NAME` = :table_name LIMIT 1"
            ),
            {"schema": schema, "table_name": table_name},
        )
        try:
            # LIMIT 1 bounds this read. first() closes without checking the
            # trailing REST queryState, which may report an opaque failure.
            rows = curs.fetchall()
        finally:
            curs.close()
        if rows:
            return True

        # File-backed tables are discovered through SHOW FILES rather than
        # INFORMATION_SCHEMA.TABLES.  Compare names in Python so that the file
        # name never becomes executable SQL.
        info_cache = kwargs.get("info_cache")
        if self.get_plugin_type(
                connection, schema, info_cache=info_cache) == 'file':
            if (
                table_name in self.get_table_names(
                    connection, schema, info_cache=info_cache)
                or table_name in self.get_view_names(
                    connection, schema, info_cache=info_cache)
            ):
                return True

            # Classpath resources (notably cp.default) are queryable but are
            # not returned by SHOW FILES.  Probe them through the same quoted
            # identifier path used for dynamic column reflection.  A DBAPI
            # failure cannot distinguish absence from permission/server errors
            # when Drill suppresses error details, so DBAPI failures propagate.
            try:
                return bool(self.get_columns(
                    connection,
                    table_name,
                    schema,
                    info_cache=info_cache,
                ))
            except exc.NoSuchTableError:
                return False
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

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        result = []
        info_cache = kw.get("info_cache")
        schema = self._schema_name(connection, schema)
        plugin_type = self.get_plugin_type(
            connection, schema, info_cache=info_cache)

        # Plugins with dynamic schemas use ** notation - query data directly
        if plugin_type in ('file', 'mongo', 'splunk'):
            quoted_file_name = self.identifier_preparer.format_drill_table(
                schema, table_name)

            # MongoDB uses ** notation - query data directly to get schema.
            # Views and plain files are both read with SELECT *, so no
            # get_view_names() round trip is needed to choose between them.
            if plugin_type == "mongo":
                q = f"SELECT `**` FROM {quoted_file_name} LIMIT 1"
            else:
                q = f"SELECT * FROM {quoted_file_name} LIMIT 1"

            # This SQL contains identifiers, not literal values.  Using
            # exec_driver_sql avoids text() treating a colon inside a quoted
            # identifier as a bind marker.
            # Drill may omit error details, so a failed SELECT cannot prove
            # absence. Keep permission, syntax, server and connection errors
            # visible instead of misclassifying them as NoSuchTableError.
            curs = connection.exec_driver_sql(q)
            try:
                column_metadata = curs.cursor.description
                # Metadata precedes rows and final queryState in REST results.
                # Exhaust this LIMIT 1 probe through SQLAlchemy so trailing
                # DBAPI errors are wrapped, and never cache failed reflection.
                curs.fetchall()
            finally:
                curs.close()

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

        # INFORMATION_SCHEMA values are literals, so both schema and table are
        # bound.  In particular, table_name is never accepted as an arbitrary
        # SELECT expression during reflection.
        query_results = connection.execute(
            text(
                "SELECT `COLUMN_NAME`, `DATA_TYPE`, `IS_NULLABLE` "
                "FROM INFORMATION_SCHEMA.`COLUMNS` "
                "WHERE `TABLE_SCHEMA` = :schema "
                "AND `TABLE_NAME` = :table_name "
                "ORDER BY `ORDINAL_POSITION`"
            ),
            {"schema": schema, "table_name": table_name},
        )

        for row in query_results:
            logger.debug(f"Getting (1) data type: {row[1].lower()}")
            drill_data_type = self.get_data_type(str(row[1]).lower())
            column = {
                "name": row[0],
                "type": drill_data_type,
                "longType": drill_data_type,
                "nullable": str(row[2]).upper() == "YES",
            }
            result.append(column)
        if not result:
            raise exc.NoSuchTableError(
                f"{schema + '.' if schema else ''}{table_name}"
            )
        logger.debug(f"Result: {result}")
        return result

    @reflection.cache
    def get_plugin_type(self, connection, plugin=None, **kw):
        """Resolve a schema path to its Drill storage plugin type.

        INFORMATION_SCHEMA.SCHEMATA only lists fully qualified workspaces, so
        an exact match alone cannot resolve a bare plugin name: on Drill
        1.21.2 ``SCHEMA_NAME = 'dfs'`` returns nothing even though ``dfs.tmp``,
        ``dfs.root`` and ``dfs.default`` all exist.  Match the plugin itself or
        any workspace beneath it, preferring an exact hit.  Both values stay
        bound; the LIKE pattern escapes ``%``, ``_`` and the escape character
        so a schema name cannot smuggle in wildcards.
        """
        if plugin is None:
            return None

        plugin = str(plugin).replace("/", ".")
        pattern = (
            plugin.replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        ) + ".%"
        rows = connection.execute(
            text(
                "SELECT `SCHEMA_NAME`, `TYPE` "
                "FROM INFORMATION_SCHEMA.`SCHEMATA` "
                "WHERE `SCHEMA_NAME` = :plugin "
                "OR `SCHEMA_NAME` LIKE :pattern ESCAPE '\\' "
                "ORDER BY `SCHEMA_NAME`"
            ),
            {"plugin": plugin, "pattern": pattern},
        ).fetchall()
        if not rows:
            return None
        for row in rows:
            if row[0] == plugin:
                return str(row[1]).lower()
        return str(rows[0][1]).lower()
