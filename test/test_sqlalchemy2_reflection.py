import datetime
import decimal
import importlib
import re
import sys
import types as python_types
import warnings

import pytest
import sqlalchemy
from sqlalchemy import Column, Integer, MetaData, Table, create_engine, inspect, select
from sqlalchemy import exc as sa_exc
from sqlalchemy import types as sa_types

from sqlalchemy_drill.base import DrillDialect
from sqlalchemy_drill.drilldbapi import ProgrammingError
from sqlalchemy_drill.drilldbapi._drilldbapi import Cursor as RestCursor
from sqlalchemy_drill.sadrill import DrillDialect_sadrill


class FakeDBAPIError(Exception):
    pass


class FakeType:
    def __init__(self, value):
        self.values = (value,)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self.rowcount = -1
        self._rows = []
        self._position = 0
        self.closed = False

    @staticmethod
    def _description(*names):
        return tuple((name, None, None, None, None, None, None) for name in names)

    def execute(self, statement, parameters=()):
        parameters = tuple(parameters)
        self.connection.state.calls.append((statement, parameters))
        if self.connection.state.failure_text in statement:
            raise FakeDBAPIError("reflection failed")

        normalized = " ".join(statement.upper().split())
        state = self.connection.state
        self._position = 0
        self.rowcount = -1

        if normalized == "SHOW DATABASES":
            self.description = self._description("SCHEMA_NAME")
            self._rows = [(name,) for name in state.schemas]
        elif "FROM INFORMATION_SCHEMA.`SCHEMATA`" in normalized:
            # Drill's SCHEMATA only lists fully qualified workspaces, so the
            # dialect matches the plugin itself OR any workspace beneath it.
            self.description = self._description("SCHEMA_NAME", "TYPE")
            plugin = parameters[0]
            self._rows = [
                (name, plugin_type)
                for name, plugin_type in sorted(state.plugin_types.items())
                if name == plugin or name.startswith(f"{plugin}.")
            ]
        elif "SELECT 1 FROM INFORMATION_SCHEMA.`TABLES`" in normalized:
            self.description = self._description("EXPR$0")
            self._rows = [(1,)] if parameters in state.tables else []
        elif "FROM INFORMATION_SCHEMA.`TABLES`" in normalized:
            self.description = self._description("name")
            schema = parameters[0]
            self._rows = [
                (table,) for candidate_schema, table in state.tables
                if candidate_schema == schema
            ]
        elif "SELECT `TABLE_SCHEMA`, `VIEW_DEFINITION` FROM INFORMATION_SCHEMA.`VIEWS`" in normalized:
            self.description = self._description("TABLE_SCHEMA", "VIEW_DEFINITION")
            self._rows = [(schema, sql) for (schema, name), sql in sorted(state.view_sql.items())
                          if name == parameters[0]]
        elif "SELECT `VIEW_DEFINITION` FROM INFORMATION_SCHEMA.`VIEWS`" in normalized:
            self.description = self._description("VIEW_DEFINITION")
            self._rows = ([(state.view_sql[parameters],)]
                          if parameters in state.view_sql else [])
        elif "FROM INFORMATION_SCHEMA.`VIEWS`" in normalized:
            self.description = self._description("TABLE_NAME")
            schema = parameters[0]
            self._rows = [
                (view,) for candidate_schema, view in state.views
                if candidate_schema == schema
            ]
        elif "FROM INFORMATION_SCHEMA.`COLUMNS`" in normalized:
            self.description = self._description(
                "COLUMN_NAME", "DATA_TYPE", "IS_NULLABLE"
            )
            self._rows = list(state.columns.get(parameters, ()))
        elif normalized.startswith("SHOW FILES FROM"):
            self.description = self._description("name")
            self._rows = [(name,) for name in state.files]
        elif normalized.startswith("SELECT"):
            self.description = tuple(
                (name, FakeType(type_name), None, None, None, None, None)
                for name, type_name in state.dynamic_columns
            )
            self._rows = []
        else:
            raise AssertionError(f"Unexpected fake SQL: {statement}")
        return self

    def executemany(self, statement, parameters):
        for parameter_set in parameters:
            self.execute(statement, parameter_set)
        return self

    def fetchone(self):
        if self._position >= len(self._rows):
            return None
        row = self._rows[self._position]
        self._position += 1
        return row

    def fetchmany(self, size=None):
        size = 1 if size is None else size
        rows = self._rows[self._position:self._position + size]
        self._position += len(rows)
        return rows

    def fetchall(self):
        rows = self._rows[self._position:]
        self._position = len(self._rows)
        return rows

    def close(self):
        self.closed = True

    def setinputsizes(self, *_args):
        return None

    def setoutputsize(self, *_args):
        return None


class FakeConnection:
    def __init__(self, state):
        self.state = state
        self.closed = False
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed = True


class FakeState:
    def __init__(self):
        self.calls = []
        self.connections = []
        self.schemas = ["cp.default", "INFORMATION_SCHEMA", "dfs.tmp", "jdbc.prod"]
        self.plugin_types = {
            "dfs.tmp": "file",
            "jdbc.prod": "jdbc",
            "mongo.analytics": "mongo",
            "kafka": "kafka",
        }
        self.tables = {
            ("jdbc.prod", "accounts"),
            ("jdbc.prod", "accounts.view.drill"),
        }
        self.views = {
            ("dfs.tmp", "saved_view"),
            ("jdbc.prod", "account_view"),
        }
        self.view_sql = {
            ("jdbc.prod", "account_view"): "SELECT `id`\nFROM `jdbc`.`prod`.`accounts`",
        }
        self.columns = {
            ("kafka", "orders_topic"): [("**", "ANY", "YES")],
            ("jdbc.prod", "accounts"): [
                ("id", "INTEGER", "NO"),
                ("display_name", "VARCHAR", "YES"),
            ]
        }
        self.files = ["orders.parquet", "saved_view.view.drill"]
        self.dynamic_columns = [("id", "BIGINT"), ("payload", "VARCHAR(100)")]
        self.failure_text = "never matches"

    def connect(self, **_kwargs):
        connection = FakeConnection(self)
        self.connections.append(connection)
        return connection


class FakeDBAPI:
    apilevel = "2.0"
    threadsafety = 2
    paramstyle = "qmark"
    Error = FakeDBAPIError
    DatabaseError = FakeDBAPIError
    OperationalError = FakeDBAPIError
    ProgrammingError = FakeDBAPIError

    def __init__(self, state):
        self.state = state

    def connect(self, **kwargs):
        return self.state.connect(**kwargs)


@pytest.fixture
def fake_engine():
    state = FakeState()
    engine = create_engine(
        "drill+sadrill://localhost:8047/dfs.tmp",
        module=FakeDBAPI(state),
    )
    try:
        yield engine, state
    finally:
        engine.dispose()


def _compiled_select(schema, table_name="events"):
    table = Table(
        table_name,
        MetaData(),
        Column("id", Integer),
        schema=schema,
    )
    return str(select(table).compile(dialect=DrillDialect_sadrill()))


@pytest.mark.parametrize("schema", [None, ""])
def test_schema_less_table_compilation_has_a_from_identifier(schema):
    assert _compiled_select(schema) == "SELECT events.id \nFROM events"


def test_qualified_table_compilation_preserves_plugin_workspace_and_file_name():
    # The FROM target keeps the full plugin/workspace path, but the column
    # qualifier must NOT repeat the schema: Drill 1.21.2 rejects
    # ``SELECT cp.`employee.json`.x FROM cp.`employee.json``` with
    # "Table 'cp' not found" and accepts the one-part qualifier below.
    assert _compiled_select("dfs.tmp", "orders.parquet") == (
        "SELECT `orders.parquet`.id \n"
        "FROM dfs.tmp.`orders.parquet`"
    )


@pytest.mark.parametrize("schema", ["cp", "dfs.tmp", "a.b.c"])
def test_column_qualifier_never_includes_the_schema(schema):
    compiled = _compiled_select(schema, "orders.parquet")
    select_clause, from_clause = compiled.split("\nFROM ")
    quoted_schema = DrillDialect_sadrill().identifier_preparer.quote_schema(schema)
    assert from_clause == f"{quoted_schema}.`orders.parquet`"
    assert quoted_schema not in select_clause
    assert select_clause == "SELECT `orders.parquet`.id "


def test_identifier_preparer_escapes_backticks_and_keeps_bind_markers_literal():
    preparer = DrillDialect_sadrill().identifier_preparer
    assert preparer.format_drill_table("bad:plugin.work?space`x", "t?`:name.json") == (
        "`bad:plugin`.`work?space``x`.`t?``:name.json`"
    )


def test_format_drill_table_rejects_the_legacy_single_string_signature():
    preparer = DrillDialect_sadrill().identifier_preparer
    # The old API was format_drill_table(path, isFile=bool).  Both spellings
    # must raise instead of silently producing a wrong identifier such as
    # ``dfs.tmp.f.csv.`False```.
    with pytest.raises(TypeError):
        preparer.format_drill_table("dfs.tmp.f.csv", isFile=True)
    with pytest.raises(TypeError):
        preparer.format_drill_table("dfs.tmp.f.csv", False)
    with pytest.raises(TypeError):
        preparer.format_drill_table("dfs.tmp.f.csv")
    # Formatting a schema on its own has a dedicated method.
    assert preparer.format_drill_schema("dfs.tmp") == "dfs.tmp"


def test_all_reflection_methods_execute_on_a_real_sa2_connection(fake_engine):
    engine, _state = fake_engine
    with engine.connect() as connection:
        dialect = connection.dialect
        assert dialect.get_schema_names(connection) == ("dfs.tmp", "jdbc.prod")
        assert dialect.get_plugin_type(connection, "jdbc.prod") == "jdbc"
        assert dialect.get_table_names(connection, "jdbc.prod") == (
            "accounts",
            "accounts",
        )
        assert dialect.get_view_names(connection, "jdbc.prod") == ("account_view",)
        assert [column["name"] for column in dialect.get_columns(
            connection, "accounts", "jdbc.prod"
        )] == ["id", "display_name"]
        assert dialect.has_table(connection, "accounts", "jdbc.prod")
        assert not dialect.has_table(connection, "missing", "jdbc.prod")


def test_file_and_mongo_reflection_use_quoted_driver_sql(fake_engine):
    engine, state = fake_engine
    with engine.connect() as connection:
        dialect = connection.dialect
        assert dialect.get_table_names(connection, "dfs.tmp") == ("orders.parquet",)
        assert [column["name"] for column in dialect.get_columns(
            connection, "orders.parquet", "dfs.tmp"
        )] == ["id", "payload"]
        assert [column["name"] for column in dialect.get_columns(
            connection, "events", "mongo.analytics"
        )] == ["id", "payload"]

    statements = [statement for statement, _parameters in state.calls]
    assert "SHOW FILES FROM dfs.tmp" in statements
    assert "SELECT * FROM dfs.tmp.`orders.parquet` LIMIT 1" in statements
    assert "SELECT `**` FROM mongo.analytics.events LIMIT 1" in statements
    # Column reflection for a file plugin used to issue an extra
    # INFORMATION_SCHEMA.VIEWS query whose result picked between two identical
    # branches.  Views and plain files are both read with SELECT *.
    assert not [s for s in statements if "INFORMATION_SCHEMA.`VIEWS`" in s]


def test_get_plugin_type_resolves_a_bare_plugin_name(fake_engine):
    engine, state = fake_engine
    state.plugin_types["dfs.root"] = "file"
    with engine.connect() as connection:
        dialect = connection.dialect
        # Drill's SCHEMATA has no bare "dfs" row -- only dfs.root and dfs.tmp.
        assert "dfs" not in state.plugin_types
        assert dialect.get_plugin_type(connection, "dfs") == "file"
        # An exact match still wins over any workspace beneath it.
        assert dialect.get_plugin_type(connection, "jdbc.prod") == "jdbc"
        assert dialect.get_plugin_type(connection, "nosuchplugin") is None

    schemata_calls = [
        (statement, parameters) for statement, parameters in state.calls
        if "INFORMATION_SCHEMA.`SCHEMATA`" in statement
    ]
    assert schemata_calls
    for statement, parameters in schemata_calls:
        # Both the exact name and the LIKE pattern are bound, and the pattern
        # escapes LIKE wildcards so a schema name cannot smuggle one in.
        assert "dfs" not in statement and "jdbc" not in statement
        assert statement.count("?") == 2
        assert parameters[1] == parameters[0].replace("_", "\\_") + ".%"


def test_get_plugin_type_escapes_like_wildcards_in_the_pattern(fake_engine):
    engine, state = fake_engine
    with engine.connect() as connection:
        connection.dialect.get_plugin_type(connection, "we_ird%plug\\in")

    pattern = [
        parameters[1] for statement, parameters in state.calls
        if "INFORMATION_SCHEMA.`SCHEMATA`" in statement
    ][-1]
    assert pattern == "we\\_ird\\%plug\\\\in.%"


def test_dynamic_reflection_preserves_opaque_dbapi_failures(fake_engine):
    engine, state = fake_engine
    # Drill can suppress the reason for a failed query. It is unsafe to
    # assume that an opaque failure means the table does not exist.
    state.failure_text = "SELECT * FROM dfs.tmp.`gone.parquet`"
    with engine.connect() as connection:
        with pytest.raises(sa_exc.DBAPIError):
            connection.dialect.get_columns(connection, "gone.parquet", "dfs.tmp")
        with pytest.raises(sa_exc.DBAPIError):
            connection.dialect.has_table(connection, "gone.parquet", "dfs.tmp")


def test_literal_reflection_values_are_bound_and_not_in_sql(fake_engine):
    engine, state = fake_engine
    malicious_schema = "prod?' OR 1=1 --:schema"
    malicious_table = "users?' OR 1=1 --:table"
    state.plugin_types[malicious_schema] = "jdbc"

    with engine.connect() as connection:
        dialect = connection.dialect
        assert dialect.get_plugin_type(connection, malicious_schema) == "jdbc"
        assert dialect.get_table_names(connection, malicious_schema) == ()
        assert dialect.get_view_names(connection, malicious_schema) == ()
        with pytest.raises(sa_exc.NoSuchTableError):
            dialect.get_columns(connection, malicious_table, malicious_schema)
        assert not dialect.has_table(connection, malicious_table, malicious_schema)

    relevant_calls = [
        (statement, parameters)
        for statement, parameters in state.calls
        if malicious_schema in parameters or malicious_table in parameters
    ]
    assert relevant_calls
    for statement, parameters in relevant_calls:
        assert malicious_schema not in statement
        assert malicious_table not in statement
        assert "?" in statement
        assert malicious_schema in parameters or malicious_table in parameters


def test_identifier_only_file_reflection_is_escaped_without_text_reparsing(fake_engine):
    engine, state = fake_engine
    malicious_schema = "dfs.we?ird`:workspace"
    malicious_table = "t?`:name.json"
    state.plugin_types[malicious_schema] = "file"

    with engine.connect() as connection:
        assert connection.dialect.get_table_names(
            connection, malicious_schema
        ) == ("orders.parquet",)
        assert len(connection.dialect.get_columns(
            connection, malicious_table, malicious_schema
        )) == 2

    assert (
        "SHOW FILES FROM dfs.`we?ird``:workspace`",
        (),
    ) in state.calls
    assert state.calls[-1] == (
        "SELECT * FROM dfs.`we?ird``:workspace`.`t?``:name.json` LIMIT 1",
        (),
    )


def test_inspector_has_table_uses_info_cache(fake_engine):
    if sqlalchemy.__version__.startswith("1."):
        pytest.skip("SQLAlchemy 1.4 Inspector.has_table does not use info_cache")

    engine, state = fake_engine
    with engine.connect() as connection:
        inspector = inspect(connection)
        assert inspector.has_table("accounts", schema="jdbc.prod")
        assert inspector.has_table("accounts", schema="jdbc.prod")
        matching_calls = [
            call for call in state.calls
            if call[0].startswith("SELECT 1 FROM INFORMATION_SCHEMA")
        ]
        assert len(matching_calls) == 1

        inspector.clear_cache()
        assert inspector.has_table("accounts", schema="jdbc.prod")
        matching_calls = [
            call for call in state.calls
            if call[0].startswith("SELECT 1 FROM INFORMATION_SCHEMA")
        ]
        assert len(matching_calls) == 2


def test_reflection_preserves_transactions_and_propagates_dbapi_errors(fake_engine):
    engine, state = fake_engine
    with engine.connect() as connection:
        transaction = connection.begin()
        connection.dialect.get_view_names(connection, "jdbc.prod")
        assert transaction.is_active
        assert connection.in_transaction()
        assert state.connections[0].commits == 0
        assert state.connections[0].rollbacks == 0

        state.failure_text = "INFORMATION_SCHEMA.`SCHEMATA`"
        with pytest.raises(sa_exc.DBAPIError, match="reflection failed"):
            connection.dialect.get_plugin_type(connection, "jdbc.prod")
        assert transaction.is_active
        transaction.rollback()


def test_rest_qmark_substitution_quotes_values_once_and_ignores_sql_literals():
    query = "SELECT '?', ?, `identifier?`, -- ?\n ? /* ? */"
    parameters = ("x?' OR 1=1 --:value", "second:value?")
    assert RestCursor.substitute_in_query(query, parameters) == (
        "SELECT '?', 'x?'' OR 1=1 --:value', `identifier?`, -- ?\n "
        "'second:value?' /* ? */"
    )


@pytest.mark.parametrize(
    "query, parameters, message",
    [
        ("SELECT ?", (), "Not enough"),
        ("SELECT 1", (1,), "Too many"),
        ("SELECT ?", {"value": 1}, "sequence"),
        # A str is a sequence of characters; treating it as a parameter list
        # silently expanded "abc" into three literals.
        ("SELECT ?, ?, ?", "abc", "sequence"),
        ("SELECT ?", b"ab", "sequence"),
        # /*/ is not a complete comment, so this template has no placeholders.
        ("SELECT /*/ ? */ 1", (1,), "Too many"),
    ],
)
def test_rest_qmark_substitution_rejects_parameter_count_or_style_errors(
    query, parameters, message
):
    with pytest.raises(ProgrammingError, match=message):
        RestCursor.substitute_in_query(query, parameters)


def test_rest_qmark_substitution_does_not_end_a_block_comment_at_slash_star_slash():
    # Regression for a scanner divergence from Drill. A scanner that consumes the
    # `/*` opener one character at a time, so the opener's `*` was re-read as a
    # closer and `/*/` looked like a finished comment.  Drill disagrees:
    # "SELECT /*/ 1 */ 2 AS v FROM (values(1))" returns 2 on Drill 1.21.2.
    # An exploit enabled by that divergence: the scanner renders a
    # parameter into what it thought was live SQL but Drill was still treating
    # as a comment, and a value containing */ then escaped it.
    hostile = "a*/ 999 AS pwned FROM (values(1)) -- "
    template = "SELECT /*/ ? */ 2 AS v FROM (values(1))"

    # There are no placeholders outside the comment, so supplying one is an
    # error rather than an injection point.
    with pytest.raises(ProgrammingError, match="Too many"):
        RestCursor.substitute_in_query(template, (hostile,))

    # With no parameters the comment is passed through untouched.
    assert RestCursor.substitute_in_query(template, ()) == template

    # A placeholder after the comment is still substituted, and the comment
    # body is preserved verbatim.
    assert RestCursor.substitute_in_query(
        "SELECT ?, /*/ ? */ ?", ("A", "B")
    ) == "SELECT 'A', /*/ ? */ 'B'"


@pytest.mark.parametrize(
    "query",
    [
        "SELECT 'unterminated",
        "SELECT `unterminated",
        'SELECT "unterminated',
        "SELECT /* unterminated",
    ],
)
def test_rest_qmark_substitution_never_substitutes_after_an_unterminated_token(query):
    # Drill will reject these statements, but the driver must not "recover"
    # and start treating the remainder as substitutable SQL.
    assert RestCursor.substitute_in_query(query + " ?", ()) == query + " ?"


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, "NULL"),
        (True, "TRUE"),
        (False, "FALSE"),
        ("it's", "'it''s'"),
        (7, "7"),
        (1.5, "1.5"),
        (decimal.Decimal("1.10"), "1.10"),
        (datetime.date(2020, 1, 2), "'2020-01-02'"),
        (datetime.time(3, 4, 5), "'03:04:05'"),
        # Drill requires 'yyyy-MM-dd HH:mm:ss'; an ISO 'T' separator raises
        # DateTimeParseException on Drill 1.21.2.
        (datetime.datetime(2020, 1, 2, 3, 4, 5), "'2020-01-02 03:04:05'"),
        (
            datetime.datetime(2020, 1, 2, 3, 4, 5, 123456),
            "'2020-01-02 03:04:05.123456'",
        ),
    ],
)
def test_rest_literal_rendering_matches_drill_syntax(value, expected):
    assert RestCursor._sql_literal(value) == expected


@pytest.mark.parametrize(
    "value, message",
    [
        # Drill parses X'..' but cannot evaluate it as a constant expression.
        (b"\xde\xad", "binary"),
        (bytearray(b"\x01"), "binary"),
        (memoryview(b"\x01"), "binary"),
        # str(float("nan")) is "nan", which Drill reads as a column reference.
        (float("nan"), "float"),
        (float("inf"), "float"),
        (float("-inf"), "float"),
        (decimal.Decimal("NaN"), "decimal"),
        (decimal.Decimal("Infinity"), "decimal"),
        (1 + 2j, "Unsupported query parameter type: complex"),
        (["a"], "Unsupported query parameter type: list"),
        # Time zones: Drill TIMESTAMP/TIME literals are naive and reject an
        # offset suffix.
        (
            datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc),
            "time zone naive",
        ),
        (
            datetime.time(3, 4, 5, tzinfo=datetime.timezone.utc),
            "time zone naive",
        ),
    ],
)
def test_rest_literal_rendering_rejects_values_drill_cannot_represent(value, message):
    # Rejecting is the point: previously these rendered into SQL that Drill
    # was guaranteed to fail on, or into a bare token like `nan`.
    with pytest.raises(ProgrammingError, match=message):
        RestCursor._sql_literal(value)


def test_jdbc_odbc_define_native_and_compatibility_dbapi_hooks(monkeypatch):
    fake_jpype_dbapi = python_types.SimpleNamespace(paramstyle="qmark")
    fake_jpype = python_types.ModuleType("jpype")
    fake_jpype.dbapi2 = fake_jpype_dbapi
    fake_jpype.isJVMStarted = lambda: True
    fake_jpype.JClass = lambda _name: object()
    fake_pyodbc = python_types.ModuleType("pyodbc")
    fake_pyodbc.paramstyle = "qmark"

    monkeypatch.setitem(sys.modules, "jpype", fake_jpype)
    monkeypatch.setitem(sys.modules, "pyodbc", fake_pyodbc)
    # Record absence too: imports below must not leak stub-bound modules.
    import sqlalchemy_drill
    for name in ("jdbc", "odbc"):
        key = "sqlalchemy_drill." + name
        monkeypatch.setitem(sys.modules, key, sys.modules.get(key))
        monkeypatch.delitem(sys.modules, key)
        monkeypatch.setattr(sqlalchemy_drill, name, None, raising=False)
        monkeypatch.delattr(sqlalchemy_drill, name)
    rest_dbapi = DrillDialect_sadrill.import_dbapi()
    jdbc = importlib.import_module("sqlalchemy_drill.jdbc")
    odbc = importlib.import_module("sqlalchemy_drill.odbc")

    for dialect_class, expected_dbapi in (
        (jdbc.DrillDialect_jdbc, fake_jpype_dbapi),
        (odbc.DrillDialect_odbc, fake_pyodbc),
    ):
        assert "import_dbapi" in dialect_class.__dict__
        assert dialect_class.import_dbapi() is expected_dbapi
        assert dialect_class.dbapi() is expected_dbapi
        # The point of the fix: neither transport may fall back to the REST
        # DB-API that DrillDialect.import_dbapi() returns.
        assert dialect_class.import_dbapi() is not rest_dbapi

    # ``driver`` is deliberately NOT asserted here.  All three dialects still
    # inherit ``driver == "rest"``, which is wrong for JDBC and ODBC; pinning
    # that value in a test would turn a known defect into a supported
    # invariant.  See the comments in jdbc.py/odbc.py.

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        jdbc_engine = create_engine("drill+jdbc://localhost:31010")
        odbc_engine = create_engine("drill+odbc:///?DSN=test")
        jdbc_engine.dispose()
        odbc_engine.dispose()

    assert isinstance(jdbc_engine.dialect, jdbc.DrillDialect_jdbc)
    assert isinstance(odbc_engine.dialect, odbc.DrillDialect_odbc)
    assert not [
        warning for warning in caught
        if "dbapi()" in str(warning.message)
        or "import_dbapi" in str(warning.message)
    ]


def test_current_sqlalchemy_uses_import_dbapi_without_deprecation_warning():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        engine = create_engine("drill+sadrill://localhost:8047/dfs.tmp")
        engine.dispose()

    dbapi_warnings = [
        warning for warning in caught
        if "dbapi()" in str(warning.message)
        or "import_dbapi" in str(warning.message)
    ]
    assert dbapi_warnings == []
    assert "import_dbapi" in DrillDialect_sadrill.__dict__


def test_complex_drill_types_remain_user_defined_without_round_trip_evidence():
    dialect = DrillDialect()
    assert dialect.get_data_type("struct") is sa_types.UserDefinedType
    assert dialect.get_data_type("array") is sa_types.UserDefinedType


def test_has_table_does_not_hide_invalidated_file_connection(fake_engine, monkeypatch):
    engine, state = fake_engine
    state.failure_text = "SELECT * FROM dfs.tmp.`gone.parquet`"
    monkeypatch.setattr(engine.dialect, "is_disconnect", lambda *_args: True)
    with engine.connect() as connection:
        with pytest.raises(sa_exc.DBAPIError) as failure:
            connection.dialect.has_table(connection, "gone.parquet", "dfs.tmp")
        assert failure.value.connection_invalidated


@pytest.mark.parametrize("target, expected", [
    ("dfs.tmp", "dfs.tmp.events"),
    ("dfs/a`b", "dfs.`a``b`.events"),
])
@pytest.mark.parametrize("source", ["tenant", "dfs/tmp", "tenant..name"])
def test_rendered_schema_translation_preserves_qualified_paths(source, target, expected):
    table = Table("events", MetaData(), Column("id", Integer), schema=source)
    sql = str(select(table.c.id).compile(
        dialect=DrillDialect_sadrill(),
        schema_translate_map={source: target},
        render_schema_translate=True,
    ))
    assert sql == f"SELECT events.id \nFROM {expected}"


def test_execute_rejects_a_scalar_parameter_as_dbapi_programming_error():
    def unexpected_submission(_query):
        raise AssertionError("invalid parameters must not reach the server")

    connection = python_types.SimpleNamespace(
        _connected=True, submit_query=unexpected_submission
    )
    cursor = RestCursor(connection)
    with pytest.raises(ProgrammingError, match="sequence"):
        cursor.execute("SELECT ?", 1)


@pytest.mark.parametrize("opener", ["--", "//"])
@pytest.mark.parametrize("ending", ["\r", "\n", "\r\n", ""])
def test_rest_drill_line_comments(opener, ending):
    # Parser.jj 8591-8594: both forms end at CR, LF, CRLF, or EOF.
    comment = opener + " ?"
    template = "SELECT " + comment + ending
    parameters = ()
    expected = template
    if ending:
        template += " ? AS v FROM (values(1))"
        parameters = (1,)
        expected += " 1 AS v FROM (values(1))"
    assert RestCursor.substitute_in_query(template, parameters) == expected
    with pytest.raises(ProgrammingError, match="Too many"):
        RestCursor.substitute_in_query(template, ("\n999 + -- ",) + parameters)
    if ending:
        with pytest.raises(ProgrammingError, match="Not enough"):
            RestCursor.substitute_in_query(template, ())


@pytest.mark.parametrize("comment", [
    "/*/ ? */", "/***/ ? */", "/**? ? */", "/**\n? */",
    "/**/", "/****/", "/* outer /* ? */",
])
def test_rest_drill_block_comment_openers(comment):
    # Parser.jj 8581-8605: /** followed by a non-slash consumes FOUR
    # characters; /**/ instead uses the two-character /* opener. No nesting.
    hostile = "a*/ 999 + -- ? ' //"
    template = "SELECT " + comment + " ?"
    assert RestCursor.substitute_in_query(template, (hostile,)) == (
        "SELECT " + comment + " 'a*/ 999 + -- ? '' //'"
    )
    with pytest.raises(ProgrammingError, match="Too many"):
        RestCursor.substitute_in_query(template, (hostile, 1))
    with pytest.raises(ProgrammingError, match="Not enough"):
        RestCursor.substitute_in_query(template, ())


@pytest.mark.parametrize("opener", ["/*/", "/***/", "/**?", "/**\r"])
def test_rest_drill_unterminated_formal_comments(opener):
    template = "SELECT " + opener + " ?"
    assert RestCursor.substitute_in_query(template, ()) == template
    with pytest.raises(ProgrammingError, match="Too many"):
        RestCursor.substitute_in_query(template, ("*/ 999 -- ",))


@pytest.fixture
def streaming_rest_engine(monkeypatch):
    import io
    import json

    import requests

    from sqlalchemy_drill.drilldbapi._drilldbapi import Connection

    state = python_types.SimpleNamespace(
        query_state="FAILED", rows=[{"v": "1"}], cursors=[], calls=[],
        failure_payload=None, leading_failure=False, transport_error=None,
        profile_calls=[], profile_payload={}, profile_status=200,
        profile_exception=None, incomplete_profiles=0,
        listings={"": [{"name": "sibling.json", "isDirectory": False,
                         "isFile": True}]}, listing_state="COMPLETED",
        listing_limit=0, listing_error=None, plugin_type="file", max_rows="0",
        table_listing=["other_collection"], probe_columns=["v"],
        probe_metadata=["INTEGER"],
    )

    def post(_url, *, data, **_kwargs):
        query = json.loads(data)["query"]
        # Cursor statements carry a leading cancellation tag comment.
        tag = re.match(r"/\* sqlalchemy-drill:[0-9a-f]{32} \*/ ", query)
        if tag:
            query = query[tag.end():]
        state.calls.append(query)
        columns, metadata, rows = state.probe_columns, state.probe_metadata, state.rows
        query_state = "COMPLETED"
        if "sys.drillbits" in query:
            columns, metadata = ["version"], ["VARCHAR"]
            rows = [{"version": "1.21.2"}]
        elif "INFORMATION_SCHEMA.`SCHEMATA`" in query:
            columns, metadata = ["SCHEMA_NAME", "TYPE"], ["VARCHAR"] * 2
            plugin_type = "jdbc" if state.probe == "exists" else state.plugin_type
            rows = ([{"SCHEMA_NAME": "cp.default", "TYPE": plugin_type}]
                    if plugin_type is not None else [])
        elif "FROM sys.options" in query:
            columns, metadata, rows = ["val"], ["VARCHAR"], [{"val": state.max_rows}]
        elif "SHOW FILES" in query:
            if state.listing_error:
                raise state.listing_error
            columns = ["name", "isDirectory", "isFile"]
            metadata = ["VARCHAR", "BIT", "BIT"]
            directory = query.removeprefix('SHOW FILES FROM cp.`default`')
            rows = state.listings.get(directory, [])
            query_state = state.listing_state
        elif "SELECT `TABLE_NAME` FROM INFORMATION_SCHEMA.`TABLES`" in query:
            columns, metadata = ["TABLE_NAME"], ["VARCHAR"]
            rows = [{"TABLE_NAME": name} for name in state.table_listing]
            query_state = state.listing_state
        elif "INFORMATION_SCHEMA.`VIEWS`" in query:
            rows = []
        elif query.startswith("SELECT 1") and state.probe == "fallback":
            rows = []
        else:
            if state.transport_error:
                raise state.transport_error
            query_state = state.query_state
        # The real Requests response and REST cursor parse rows before the
        # opaque trailing state, matching StreamingHttpConnection.finish().
        response = requests.Response()
        response.status_code = 200
        payload = {
            "columns": columns, "metadata": metadata, "rows": rows,
            "queryState": query_state,
            "attemptedAutoLimit": state.listing_limit if "SHOW FILES" in query else 0,
        }
        if query_state == "FAILED" and state.failure_payload is not None:
            if state.leading_failure:
                payload = dict(state.failure_payload)
            else:
                payload.update(state.failure_payload)
        response.raw = io.BytesIO(json.dumps(payload).encode())
        return response

    def get(url, **kwargs):
        state.profile_calls.append((url, kwargs))
        if state.profile_exception:
            raise state.profile_exception
        response = requests.Response()
        response.status_code = state.profile_status
        payload = state.profile_payload
        if len(state.profile_calls) <= state.incomplete_profiles:
            payload = {"state": 4}
        response.raw = io.BytesIO(json.dumps(payload).encode())
        return response

    original_cursor = Connection.cursor

    def cursor(connection):
        result = original_cursor(connection)
        state.cursors.append(result)
        return result

    monkeypatch.setattr(Connection, "cursor", cursor)
    session = requests.Session()
    monkeypatch.setattr(session, "post", post)
    monkeypatch.setattr(session, "get", get)
    engine = create_engine(
        "drill+sadrill://localhost:8047/cp.default",
        creator=lambda: Connection("localhost", 8047, "http://", None, session),
    )
    try:
        yield engine, state
    finally:
        engine.dispose()


@pytest.mark.parametrize("probe", ["columns", "exists", "fallback"])
@pytest.mark.parametrize("rows", [[], [{"v": "1"}]])
def test_rest_reflection_exhausts_trailing_state(streaming_rest_engine, probe, rows):
    engine, state = streaming_rest_engine
    state.probe, state.rows = probe, rows
    with engine.connect() as connection:
        dialect = connection.dialect
        method = dialect.get_columns if probe == "columns" else dialect.has_table
        cache = {}
        for _ in range(2):
            with pytest.raises(sa_exc.DBAPIError, match="query state is FAILED"):
                method(connection, "resource.json", "cp.default", info_cache=cache)
            assert all(not cursor._is_open for cursor in state.cursors)
            assert not [key for key in cache if key[0] in ("get_columns", "has_table")]
        # Identical payload fails on normal fetch-to-exhaustion too. In the
        # one-row case, merely fetching that row has not checked final state.
        result = connection.exec_driver_sql("SELECT * FROM cp.default.t LIMIT 1")
        cursor = result.cursor
        try:
            assert cursor.description[0][0] == "v"
            assert "queryState" not in cursor.result_md
            if rows:
                assert result.fetchone() == ("1",)
                assert "queryState" not in cursor.result_md
            with pytest.raises(sa_exc.DBAPIError, match="query state is FAILED"):
                result.fetchall()
        finally:
            result.close()
        assert not cursor._is_open

        state.query_state = "COMPLETED"
        value = method(connection, "resource.json", "cp.default", info_cache=cache)
        if probe == "columns":
            assert [column["name"] for column in value] == ["v"]
        else:
            assert value is (probe == "fallback" or bool(rows))
        assert all(not cursor._is_open for cursor in state.cursors)
        calls = len(state.calls)
        assert method(connection, "resource.json", "cp.default", info_cache=cache) == value
        assert len(state.calls) == calls


# Sanitized diagnostic shape established against live Drill 1.21.2. The same
# Object-not-found diagnostic was returned for an existing file under chmod 000.
_MISSING_OBJECT = (
    "VALIDATION ERROR: From line 1, column 15 to line 1, column 49: "
    "Object 'sc121464_denied/data.json' not found within 'dfs.tmp'\n\n"
)


def _failed_reflection(state, message, verbose=False, leading=True):
    state.probe = "fallback"
    state.leading_failure = leading
    state.failure_payload = {"queryId": "test-query-id", "queryState": "FAILED"}
    if verbose:
        # Verbose REST messages omit the class and repeat the diagnostic;
        # only the profile carries the authoritative VALIDATION ERROR prefix.
        diagnostic = message.split(" ERROR: ", 1)[-1].strip()
        cause = diagnostic.split(": ", 1)[-1]
        state.failure_payload["errorMessage"] = diagnostic + ": " + cause
    state.profile_payload = {"error": message}


def _reflect(engine, operation, table_name="resource.json"):
    with engine.connect() as connection:
        if operation == "autoload":
            return Table(table_name, MetaData(), schema="cp.default",
                         autoload_with=connection)
        inspector = inspect(connection)
        return getattr(inspector, operation)(table_name, schema="cp.default")


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize("leading", [False, True])
def test_missing_object_reflection_contract(streaming_rest_engine, operation,
                                            verbose, leading):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT, verbose, leading)
    if operation == "has_table":
        assert _reflect(engine, operation) is False
    else:
        with pytest.raises(sa_exc.NoSuchTableError):
            _reflect(engine, operation)
    # One profile read, bounded by the remaining 30 s poll budget.
    assert [url for url, _ in state.profile_calls] == [
        "http://localhost:8047/profiles/test-query-id.json"]
    assert 29 < state.profile_calls[0][1]["timeout"] <= 30
    assert all(not cursor._is_open for cursor in state.cursors)


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize("message", [
    "PARSE ERROR: Encountered unexpected token at line 1, column 1",
    "PARSE ERROR: Object 'resource.json' not found",
    "PERMISSION ERROR: Object 'resource.json' not found",
    "SYSTEM ERROR: Object 'resource.json' not found",
    "VALIDATION ERROR: Column 'bad_column' not found in any table",
    "VALIDATION ERROR: Cannot apply operator to arguments",
    "SYSTEM ERROR: failure\nVALIDATION ERROR: Object 'resource.json' not found",
    "Object 'resource.json' not found",
    _MISSING_OBJECT.removeprefix("VALIDATION ERROR: "),
])
def test_non_missing_failures_never_become_absence(streaming_rest_engine,
                                                  operation, verbose, message):
    engine, state = streaming_rest_engine
    _failed_reflection(state, message, verbose)
    with pytest.raises(sa_exc.DatabaseError, match="query state is FAILED") as caught:
        _reflect(engine, operation)
    # Preserve the actual DBAPI failure, not a replacement classification error.
    assert caught.value.orig._drill_cursor.result_md == state.failure_payload
    assert len(state.profile_calls) == 1


def test_classified_inline_message_needs_no_profile(streaming_rest_engine):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT)
    state.failure_payload["errorMessage"] = _MISSING_OBJECT
    assert _reflect(engine, "has_table") is False
    assert state.profile_calls == []


@pytest.mark.parametrize("query_id", [None, "", 123])
def test_unclassified_inline_message_without_query_id_is_not_absence(
        streaming_rest_engine, query_id):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT, verbose=True)
    state.failure_payload["queryId"] = query_id
    with pytest.raises(sa_exc.DatabaseError):
        _reflect(engine, "has_table")
    assert state.profile_calls == []


@pytest.mark.parametrize("message", [None, "", 123, {}, "unclassified error"])
def test_unclassified_inline_message_still_uses_profile(streaming_rest_engine, message):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT)
    state.failure_payload["errorMessage"] = message
    assert _reflect(engine, "has_table") is False
    assert len(state.profile_calls) == 1


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
@pytest.mark.parametrize("verbose", [False, True])
def test_dead_server_is_never_absence(streaming_rest_engine, verbose, operation):
    from requests import ConnectionError

    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT, verbose)
    state.transport_error = ConnectionError("server is unavailable")
    # Transport failures are DB-API OperationalErrors that SQLAlchemy treats
    # as disconnects; they are never classified as absence.
    with pytest.raises(sa_exc.OperationalError) as caught:
        _reflect(engine, operation)
    assert caught.value.orig.__cause__ is state.transport_error
    assert caught.value.connection_invalidated
    assert state.profile_calls == []


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
@pytest.mark.parametrize("problem", ["http", "transport", "json", "no_error", "not_object"])
@pytest.mark.parametrize("verbose", [False, True])
def test_unavailable_profile_preserves_failure(
        streaming_rest_engine, verbose, operation, problem, monkeypatch):
    from requests import ConnectionError

    monkeypatch.setattr("sqlalchemy_drill.base.sleep", lambda _delay: None)
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT, verbose)
    if problem == "http":
        # Even an error response containing the diagnostic is not evidence.
        state.profile_status = 403
    elif problem == "transport":
        state.profile_exception = ConnectionError("profile unavailable")
    elif problem == "json":
        state.profile_exception = ValueError("invalid JSON")
    elif problem == "no_error":
        state.profile_payload = {}
    else:
        state.profile_payload = []
    with pytest.raises(sa_exc.DatabaseError, match="query state is FAILED"):
        _reflect(engine, operation)
    assert len(state.profile_calls) == (7 if problem == "no_error" else 1)


@pytest.mark.parametrize("verbose", [False, True])
def test_profile_publication_can_lag_failed_query(
        streaming_rest_engine, verbose, monkeypatch):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT, verbose)
    state.incomplete_profiles = 2
    delays = []
    monkeypatch.setattr("sqlalchemy_drill.base.sleep", delays.append)
    assert _reflect(engine, "has_table") is False
    assert len(state.profile_calls) == 3
    assert delays == [0.1, 0.2]


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
@pytest.mark.parametrize("verbose", [False, True])
def test_busy_server_profile_lag_still_proves_absence(
        streaming_rest_engine, verbose, operation, monkeypatch):
    # A busy server published the failed profile's error only after 0.3 s:
    # the opaque REST failure must still become proven absence, not a
    # DatabaseError, once the authoritative profile arrives.
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT, verbose)
    state.incomplete_profiles = 5
    delays = []
    monkeypatch.setattr("sqlalchemy_drill.base.sleep", delays.append)
    if operation == "has_table":
        assert _reflect(engine, operation) is False
    else:
        with pytest.raises(sa_exc.NoSuchTableError):
            _reflect(engine, operation)
    assert len(state.profile_calls) == 6
    assert delays == [0.1, 0.2, 0.4, 0.8, 1.6]
    assert sum(delays) > 0.3


def test_profile_that_never_publishes_an_error_is_bounded(
        streaming_rest_engine, monkeypatch):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT)
    state.incomplete_profiles = 100
    delays = []
    monkeypatch.setattr("sqlalchemy_drill.base.sleep", delays.append)
    with pytest.raises(sa_exc.DatabaseError, match="query state is FAILED"):
        _reflect(engine, "has_table")
    assert len(state.profile_calls) == 7
    assert sum(delays) == pytest.approx(5.1)


@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
def test_permission_denied_raises(streaming_rest_engine, verbose, operation):
    # The parent lists the directory, but the denied directory's successful
    # empty listing is indistinguishable from a readable empty directory.
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT, verbose)
    state.listings = {"": [{"name": "sc121464_denied", "isDirectory": True,
                            "isFile": False}], ".`./sc121464_denied`": []}
    with pytest.raises(sa_exc.DatabaseError) as caught:
        _reflect(engine, operation, "sc121464_denied/data.json")
    assert caught.value.orig._drill_cursor.result_md == state.failure_payload


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
@pytest.mark.parametrize("problem", [
    "empty", "present", "view", "malformed", "failed", "transport", "limited",
    "mongo", "unknown_schema", "server_limit",
])
@pytest.mark.parametrize("verbose", [False, True])
def test_unproven_absence_preserves_original_error(streaming_rest_engine, verbose,
                                                  operation, problem):
    from requests import ConnectionError

    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT, verbose)
    if problem == "empty":
        state.listings = {}
    elif problem in ("present", "view"):
        name = "resource.json" + (".view.drill" if problem == "view" else "")
        state.listings = {"": [{"name": name, "isDirectory": False, "isFile": True}]}
    elif problem == "malformed":
        state.listings = {"": [{"name": None}]}
    elif problem == "failed":
        state.listing_state = "FAILED"
    elif problem == "transport":
        state.listing_error = ConnectionError("listing unavailable")
    elif problem == "limited":
        state.listing_limit = 1
    elif problem == "server_limit":
        state.max_rows = "1"
    elif problem == "mongo":
        # An empty collection listing cannot prove absence.
        state.plugin_type = "mongo"
        state.table_listing = []
    elif problem == "unknown_schema":
        state.plugin_type = None
    with pytest.raises(sa_exc.DatabaseError) as caught:
        _reflect(engine, operation)
    assert caught.value.orig._drill_cursor.result_md == state.failure_payload
    # A transport failure invalidates the connection (a disconnect); its
    # never-streamed cursor is discarded with it rather than closed.
    assert all(not cursor._is_open or not cursor.connection._connected
               for cursor in state.cursors)
    assert all(cursor._row_stream is None or not cursor._is_open
               for cursor in state.cursors)


@pytest.mark.parametrize("name", ["../x", "/x", "a//b", "a/./b", "a*", "a?b",
                                  "a[b]", "a{b,c}", "a:b", "a\\b"])
def test_nonliteral_paths_are_not_proven_absent(streaming_rest_engine, name):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT)
    with pytest.raises(sa_exc.DatabaseError):
        _reflect(engine, "has_table", name)
    assert not any("SHOW FILES" in query for query in state.calls)


@pytest.mark.parametrize("missing_parent", [False, True])
def test_nested_absence_requires_readable_ancestor(streaming_rest_engine, missing_parent):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT)
    if not missing_parent:
        state.listings[""].append({"name": "dir", "isDirectory": True, "isFile": False})
        state.listings[".`./dir`"] = [{"name": "sibling.json", "isDirectory": False,
                                   "isFile": True}]
    assert _reflect(engine, "has_table", "dir/missing.json") is False


@pytest.mark.parametrize("verbose", [False, True])
def test_success_and_ordinary_query_errors_never_fetch_profiles(
        streaming_rest_engine, verbose):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT, verbose)
    with engine.connect() as connection:
        with pytest.raises(sa_exc.DatabaseError):
            connection.exec_driver_sql("SELECT * FROM cp.default.t LIMIT 1")
    assert state.profile_calls == []
    state.query_state = "COMPLETED"
    assert _reflect(engine, "has_table") is True
    assert state.profile_calls == []


@pytest.mark.parametrize("column_type,ticks,expected", [
    ("TIMESTAMP", 0, datetime.datetime(1970, 1, 1)),
    ("DATE", 0, datetime.date(1970, 1, 1)),
    ("TIME", 0, datetime.time(0, 0)),
    ("TIMESTAMP", 1790426096789,
     datetime.datetime(2026, 9, 26, 12, 34, 56, 789000)),
    ("TIME", 45296789, datetime.time(12, 34, 56, 789000)),
    ("TIMESTAMP", -1000, datetime.datetime(1969, 12, 31, 23, 59, 59)),
    ("DATE", -2208988800000, datetime.date(1900, 1, 1)),
    ("TIMESTAMP", None, None),
    ("DATE", None, None),
    ("TIME", None, None),
])
def test_rest_temporal_values_keep_zero_and_milliseconds(column_type, ticks, expected):
    # Drill >= 1.19 REST sends temporal values as epoch milliseconds. Zero is
    # midnight / the epoch, not NULL, and the millisecond fraction is data.
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    decode = {"DATE": _drilldbapi.DateFromTicks, "TIME": _drilldbapi.TimeFromTicks,
              "TIMESTAMP": _drilldbapi.TimestampFromTicks}[column_type]
    got = decode(ticks)
    assert got == expected and type(got) is type(expected)


def test_view_definition_reads_bound_information_schema(fake_engine):
    engine, state = fake_engine
    inspector = sqlalchemy.inspect(engine)
    assert inspector.get_view_definition("account_view", "jdbc.prod") == (
        "SELECT `id`\nFROM `jdbc`.`prod`.`accounts`"
    )
    statement, parameters = state.calls[-1]
    assert "account_view" not in statement and "jdbc.prod" not in statement
    assert parameters == ("jdbc.prod", "account_view")
    with pytest.raises(sa_exc.NoSuchTableError):
        inspector.get_view_definition("no_such_view", "jdbc.prod")


@pytest.mark.parametrize("value,expected", [
    ("true", True), ("True", True), ("1", True),
    ("false", False), ("False", False), ("0", False),
    ("/etc/ssl/certs/ca.pem", "/etc/ssl/certs/ca.pem"),
])
def test_rest_verify_ssl_url_value_is_a_flag_or_a_ca_bundle_path(value, expected):
    # requests reads every string verify value as a CA bundle path, so the
    # natural verify_ssl=true URL spelling must reach it as a boolean.
    from sqlalchemy.engine import make_url

    _args, kwargs = DrillDialect_sadrill().create_connect_args(
        make_url(f"drill+sadrill://h:8047/dfs/tmp?use_ssl=true&verify_ssl={value}"))
    assert kwargs["verify_ssl"] == expected and type(kwargs["verify_ssl"]) is type(expected)


def test_rest_verify_ssl_is_absent_unless_configured():
    from sqlalchemy.engine import make_url

    _args, kwargs = DrillDialect_sadrill().create_connect_args(
        make_url("drill+sadrill://h:8047/dfs/tmp?use_ssl=true"))
    assert "verify_ssl" not in kwargs


class _RecordingSession:
    """Minimal requests.Session stand-in that records request timeouts."""

    def __init__(self, fail_on=None):
        self.timeouts = []
        self.fail_on = fail_on
        self.verify = False

    def post(self, url, **kwargs):
        import io
        import json

        import requests

        self.timeouts.append(kwargs.get("timeout"))
        if self.fail_on and self.fail_on in url and len(self.timeouts) > 1:
            raise requests.exceptions.ReadTimeout("read timed out")
        response = requests.Response()
        response.status_code = 200
        body = {"columns": ["version"], "metadata": ["VARCHAR"],
                "rows": [{"version": "1.21.2"}], "queryState": "COMPLETED"}
        response.raw = io.BytesIO(json.dumps(body).encode())
        return response


@pytest.mark.parametrize("configured,expected", [(None, None), ("2.5", 2.5), (7, 7.0)])
def test_rest_request_timeout_is_opt_in_and_reaches_every_request(
        monkeypatch, configured, expected):
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    session = _RecordingSession()
    monkeypatch.setattr(_drilldbapi, "Session", lambda: session)
    kwargs = {} if configured is None else {"request_timeout": configured}
    connection = _drilldbapi.connect("h", 8047, **kwargs)
    connection.submit_query("SELECT 1")
    # login probe, version query, then the explicit query
    assert session.timeouts == [expected] * 3


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "soon"])
def test_rest_request_timeout_rejects_non_positive_or_non_numeric(value):
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    with pytest.raises(_drilldbapi.ProgrammingError, match="request_timeout"):
        _drilldbapi.connect("h", 8047, request_timeout=value)


def test_rest_request_timeout_raises_operational_error(monkeypatch):
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    session = _RecordingSession(fail_on="query.json")
    monkeypatch.setattr(_drilldbapi, "Session", lambda: session)
    connection = _drilldbapi.Connection(
        "h", 8047, "http://", None, session, request_timeout=1.5)
    with pytest.raises(_drilldbapi.OperationalError, match="timed out after 1.5 s"):
        connection.submit_query("SELECT 1")


def test_rest_request_timeout_url_value_reaches_connect():
    from sqlalchemy.engine import make_url

    _args, kwargs = DrillDialect_sadrill().create_connect_args(
        make_url("drill+sadrill://h:8047/dfs/tmp?request_timeout=30"))
    assert kwargs["request_timeout"] == "30"
    _args, kwargs = DrillDialect_sadrill().create_connect_args(
        make_url("drill+sadrill://h:8047/dfs/tmp"))
    assert "request_timeout" not in kwargs


class _CancelSession(_RecordingSession):
    def __init__(self, reply="Cancelled query q-1 on locally running node.",
                 status=200):
        super().__init__()
        self.reply, self.status, self.gets = reply, status, []

    def get(self, url, **kwargs):
        import io

        import requests

        self.gets.append((url, kwargs))
        response = requests.Response()
        response.status_code = self.status
        response.raw = io.BytesIO(self.reply.encode())
        return response


@pytest.mark.parametrize("reply,expected", [
    ("Cancelled query q/1 on locally running node.", True),
    ("Query q/1 canceled on node drillbit-2.", True),
    ("Attempted to cancel query q/1 on drillbit-2 but the query is no longer "
     "active on that node.", False),
    ("Failure attempting to cancel query q/1.  Unable to find information about "
     "where query is actively running.", False),
])
def test_rest_cancel_query_uses_the_cancel_endpoint(reply, expected):
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    session = _CancelSession(reply)
    connection = _drilldbapi.Connection("h", 8047, "http://", None, session)
    assert connection.cancel_query("q/1") is expected
    assert session.gets == [("http://h:8047/profiles/cancel/q%2F1", {"timeout": 30})]


def test_rest_cancel_query_http_failure_is_operational_error():
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    connection = _drilldbapi.Connection(
        "h", 8047, "http://", None, _CancelSession("denied", status=403))
    with pytest.raises(_drilldbapi.OperationalError, match="cancel request failed"):
        connection.cancel_query("q-1")


class _RunningSession(_CancelSession):
    """Serves /profiles/running.json and the cancel endpoint."""

    def __init__(self, running, reply="Cancelled query q-7 on locally running node."):
        super().__init__(reply)
        self.running = running

    def get(self, url, **kwargs):
        import io
        import json

        import requests

        if url.endswith("/profiles/running.json"):
            self.gets.append((url, kwargs))
            response = requests.Response()
            response.status_code = 200
            response.raw = io.BytesIO(json.dumps({"runningQueries": self.running}).encode())
            return response
        return super().get(url, **kwargs)


@pytest.mark.parametrize("url_value,expected", [(None, True), ("false", False), ("/ca.pem", "/ca.pem")])
def test_rest_tls_is_verified_by_default(monkeypatch, url_value, expected):
    from sqlalchemy.engine import make_url

    from sqlalchemy_drill.drilldbapi import _drilldbapi

    session = _RecordingSession()
    monkeypatch.setattr(_drilldbapi, "Session", lambda: session)
    query = "use_ssl=true" + ("" if url_value is None else f"&verify_ssl={url_value}")
    _args, kwargs = DrillDialect_sadrill().create_connect_args(
        make_url(f"drill+sadrill://h:8047/dfs/tmp?{query}"))
    kwargs.pop("db")
    _drilldbapi.connect(**kwargs)
    assert session.verify == expected


def test_transport_failures_are_disconnects():
    from requests import ConnectionError

    from sqlalchemy_drill.drilldbapi import _drilldbapi

    dialect = DrillDialect_sadrill()
    session = _RecordingSession()
    connection = _drilldbapi.Connection("h", 8047, "http://", None, session)

    def fail(url, **kwargs):
        raise ConnectionError("connection refused")

    session.post = fail
    with pytest.raises(_drilldbapi.OperationalError) as caught:
        connection.submit_query("SELECT 1")
    assert isinstance(caught.value, _drilldbapi.TransportError)
    assert dialect.is_disconnect(caught.value, None, None)
    closed = _drilldbapi.ConnectionClosedException("closed")
    assert dialect.is_disconnect(closed, None, None)
    assert not dialect.is_disconnect(_drilldbapi.DatabaseError("syntax", None), None, None)


def test_stream_wrapper_reads_in_chunks_and_wraps_stream_failures():
    from requests import ConnectionError

    from sqlalchemy_drill.drilldbapi import _drilldbapi

    class Resp:
        def __init__(self, chunks):
            self.chunks, self.sizes = chunks, []

        def iter_content(self, chunk_size):
            self.sizes.append(chunk_size)
            for chunk in self.chunks:
                if isinstance(chunk, Exception):
                    raise chunk
                yield chunk

    resp = Resp([b"abc", b"defgh"])
    wrapper = _drilldbapi.RequestsStreamWrapper(resp)
    assert wrapper.read(2) == b"ab"
    assert resp.sizes == [65536]
    assert wrapper.read(10) == b"cdefgh"
    assert wrapper.read(10) == b""
    broken = _drilldbapi.RequestsStreamWrapper(Resp([b"x", ConnectionError("reset")]))
    with pytest.raises(_drilldbapi.TransportError, match="result stream"):
        broken.read(5)

def test_streamed_query_body_is_not_read_into_memory():
    # Evaluating Response.text downloads the whole body, so a streamed
    # query must never touch it (not even for debug logging).
    import io
    import json

    import requests

    from sqlalchemy_drill.drilldbapi import _drilldbapi

    touched = []

    class Guarded(requests.Response):
        @property
        def text(self):
            touched.append(True)
            return super().text

    class Session(_RecordingSession):
        def post(self, url, **kwargs):
            response = Guarded()
            response.status_code = 200
            if "sys.drillbits" in kwargs["data"]:
                body = {"columns": ["version"], "metadata": ["VARCHAR"],
                        "rows": [{"version": "1.21.2"}], "queryState": "COMPLETED"}
            else:
                body = {"queryId": "q", "columns": ["v"], "metadata": ["INTEGER"],
                        "rows": [{"v": 1}, {"v": 2}], "queryState": "COMPLETED"}
            response.raw = io.BytesIO(json.dumps(body).encode())
            return response

    connection = _drilldbapi.Connection("h", 8047, "http://", None, Session())
    touched.clear()
    cursor = connection.cursor()
    cursor.execute("SELECT v FROM t")
    assert cursor.fetchall() == [(1,), (2,)]
    assert touched == []


def test_dynamic_schema_plugin_reflects_real_columns_not_the_placeholder(fake_engine):
    # Kafka (and other dynamic-schema plugins) list a single `**` column in
    # INFORMATION_SCHEMA; the real columns come from a LIMIT 1 probe.
    engine, state = fake_engine
    with engine.connect() as connection:
        columns = connection.dialect.get_columns(connection, "orders_topic", "kafka")
    assert [column["name"] for column in columns] == ["id", "payload"]
    assert any(statement == "SELECT * FROM kafka.orders_topic LIMIT 1"
               for statement, _ in state.calls)


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
@pytest.mark.parametrize("verbose", [False, True])
def test_missing_mongo_collection_is_proven_absent_from_a_complete_listing(
        streaming_rest_engine, verbose, operation):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT, verbose)
    state.plugin_type = "mongo"
    if operation == "has_table":
        assert _reflect(engine, operation) is False
    else:
        with pytest.raises(sa_exc.NoSuchTableError):
            _reflect(engine, operation)


@pytest.mark.parametrize("problem", ["empty", "listed", "failed", "server_limit"])
def test_unprovable_mongo_absence_keeps_the_original_error(streaming_rest_engine, problem):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT)
    state.plugin_type = "mongo"
    if problem == "empty":
        state.table_listing = []
    elif problem == "listed":
        state.table_listing = ["resource.json"]
    elif problem == "failed":
        state.listing_state = "FAILED"
    else:
        state.max_rows = "1"
    with pytest.raises(sa_exc.DatabaseError) as caught:
        _reflect(engine, "get_columns")
    assert not isinstance(caught.value, sa_exc.NoSuchTableError)


def test_profile_poll_shares_the_request_timeout_budget(streaming_rest_engine, monkeypatch):
    # Every profile read and backoff sleep comes out of one budget: the
    # connection's request_timeout (here 2 s), never 7 x 30 s.
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT)
    state.incomplete_profiles = 100
    clock = [0.0]
    monkeypatch.setattr("sqlalchemy_drill.base.monotonic", lambda: clock[0])

    def fake_sleep(delay):
        clock[0] += delay

    monkeypatch.setattr("sqlalchemy_drill.base.sleep", fake_sleep)
    with engine.connect() as connection:
        connection.connection.dbapi_connection._request_timeout = 2
        with pytest.raises(sa_exc.DatabaseError):
            connection.dialect.has_table(connection, "resource.json", "cp.default")
    timeouts = [kwargs["timeout"] for _, kwargs in state.profile_calls]
    assert timeouts and all(0 < t <= 2 for t in timeouts)
    assert clock[0] < 2


def test_profile_poll_without_request_timeout_is_bounded_to_30_seconds(
        streaming_rest_engine, monkeypatch):
    engine, state = streaming_rest_engine
    _failed_reflection(state, _MISSING_OBJECT)
    state.incomplete_profiles = 100
    clock = [0.0]
    monkeypatch.setattr("sqlalchemy_drill.base.monotonic", lambda: clock[0])

    def slow_get_then_sleep(delay):
        clock[0] += delay

    original_get_calls = state.profile_calls
    monkeypatch.setattr("sqlalchemy_drill.base.sleep", slow_get_then_sleep)

    # Model each profile read taking 12 s of wall clock.
    import sqlalchemy_drill.base as base_module
    real_quote = base_module.quote

    def quote_and_advance(value, safe=""):
        clock[0] += 12
        return real_quote(value, safe=safe)

    monkeypatch.setattr("sqlalchemy_drill.base.quote", quote_and_advance)
    with pytest.raises(sa_exc.DatabaseError):
        _reflect(engine, "has_table")
    assert len(original_get_calls) <= 3
    assert clock[0] <= 30 + 12



def _tagging_connection(session):
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    queries = []
    original = session.post

    def post(url, **kwargs):
        import json

        queries.append(json.loads(kwargs["data"])["query"])
        return original(url, **kwargs)

    session.post = post
    return _drilldbapi.Connection("h", 8047, "http://", None, session,
                                  request_timeout=session_timeout(session)), queries


def session_timeout(session):
    return getattr(session, "request_timeout", None)


def test_each_statement_carries_its_own_cancellation_tag():
    connection, queries = _tagging_connection(_RecordingSession())
    cursor = connection.cursor()
    assert cursor.query_tag is None
    cursor.execute("SELECT 1")
    first = cursor.query_tag
    cursor.execute("SELECT 2")
    second = cursor.query_tag
    assert re.fullmatch(r"[0-9a-f]{32}", first) and first != second
    assert queries[-2:] == [f"/* sqlalchemy-drill:{first} */ SELECT 1",
                            f"/* sqlalchemy-drill:{second} */ SELECT 2"]


def test_cursor_cancel_targets_only_its_latest_statement():
    session = _RunningSession([])
    session.request_timeout = 4
    connection, _ = _tagging_connection(session)
    cursor = connection.cursor()
    cursor.execute("SELECT 1")
    old = cursor.query_tag
    cursor.execute("SELECT 2")
    # The earlier statement is (hypothetically) still listed; it must never
    # be the one cancelled.
    session.running = [
        {"queryId": "q-old", "query": f"/* sqlalchemy-drill:{old} */ SELECT 1"},
        {"queryId": "q-7", "query": f"/* sqlalchemy-drill:{cursor.query_tag} */ SELECT 2"},
    ]
    session.gets.clear()
    assert cursor.cancel() is True
    assert session.gets == [
        ("http://h:8047/profiles/running.json", {"timeout": 4}),
        ("http://h:8047/profiles/cancel/q-7", {"timeout": 4}),
    ]


def test_cursor_cancel_without_a_statement_or_running_query_returns_false(monkeypatch):
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    monkeypatch.setattr(_drilldbapi, "sleep", lambda _s: None)
    session = _RunningSession([{"queryId": "q-x", "query": "SELECT 1"}])
    connection, _ = _tagging_connection(session)
    cursor = connection.cursor()
    assert cursor.cancel() is False
    assert session.gets == []
    cursor.execute("SELECT 1")
    assert cursor.cancel() is False
    assert [url for url, _ in session.gets] == ["http://h:8047/profiles/running.json"] * 3


def test_ambiguous_tag_is_never_cancelled():
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    session = _RunningSession([])
    connection, _ = _tagging_connection(session)
    cursor = connection.cursor()
    cursor.execute("SELECT 1")
    tagged = f"/* sqlalchemy-drill:{cursor.query_tag} */ SELECT 1"
    session.running = [{"queryId": "a", "query": tagged}, {"queryId": "b", "query": tagged}]
    with pytest.raises(_drilldbapi.OperationalError, match="carry tag"):
        cursor.cancel()
    assert not any("/cancel/" in url for url, _ in session.gets)


def test_request_timeout_cancels_the_server_query():
    import requests

    from sqlalchemy_drill.drilldbapi import _drilldbapi

    session = _RunningSession([])
    session.request_timeout = 3
    connection, queries = _tagging_connection(session)
    cursor = connection.cursor()
    posted = session.post

    def post(url, **kwargs):
        posted(url, **kwargs)
        tag = re.match(r"/\* sqlalchemy-drill:([0-9a-f]{32}) \*/", queries[-1]).group(1)
        session.running = [{"queryId": "q-slow",
                            "query": f"/* sqlalchemy-drill:{tag} */ SELECT slow"}]
        raise requests.exceptions.ReadTimeout("read timed out")

    session.post = post
    with pytest.raises(_drilldbapi.TransportError, match="timed out after 3"):
        cursor.execute("SELECT slow")
    assert ("http://h:8047/profiles/cancel/q-slow", {"timeout": 3}) in session.gets


def test_connection_failure_does_not_attempt_a_cancel():
    import requests

    from sqlalchemy_drill.drilldbapi import _drilldbapi

    session = _RunningSession([])
    session.request_timeout = 3
    connection, _ = _tagging_connection(session)

    def refused(url, **kwargs):
        raise requests.exceptions.ConnectionError("refused")

    session.post = refused
    with pytest.raises(_drilldbapi.TransportError):
        connection.cursor().execute("SELECT 1")
    assert session.gets == []



def test_view_definition_without_any_schema_searches_all_schemas():
    # No schema argument and no URL database used to bind TABLE_SCHEMA = NULL,
    # which never matches.
    state = FakeState()
    state.view_sql[("dfs.tmp", "saved_view")] = "SELECT 1"
    engine = create_engine("drill+sadrill://localhost:8047", module=FakeDBAPI(state))
    try:
        inspector = sqlalchemy.inspect(engine)
        assert inspector.get_view_definition("account_view") == (
            "SELECT `id`\nFROM `jdbc`.`prod`.`accounts`"
        )
        assert all(None not in parameters for _statement, parameters in state.calls)
        with pytest.raises(sa_exc.NoSuchTableError):
            inspector.get_view_definition("no_such_view")
        state.view_sql[("jdbc.prod", "saved_view")] = "SELECT 2"
        with pytest.raises(sa_exc.InvalidRequestError, match="several schemas"):
            sqlalchemy.inspect(engine).get_view_definition("saved_view")
    finally:
        engine.dispose()


@pytest.mark.parametrize("verify,where", [(True, "the system trust store"),
                                          ("/etc/drill/ca.pem", "the CA bundle '/etc/drill/ca.pem'")])
def test_untrusted_certificate_error_names_the_migration(monkeypatch, verify, where):
    import requests

    from sqlalchemy_drill.drilldbapi import _drilldbapi

    session = _RecordingSession()

    def untrusted(url, **kwargs):
        raise requests.exceptions.SSLError(
            "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate")

    session.post = untrusted
    monkeypatch.setattr(_drilldbapi, "Session", lambda: session)
    with pytest.raises(_drilldbapi.TransportError) as caught:
        _drilldbapi.connect("h", 8047, use_ssl=True, verify_ssl=verify)
    message = str(caught.value)
    assert "TLS certificate verification failed" in message and where in message
    assert "verify_ssl=<path to the CA bundle" in message
    # The driver never retries without verification.
    assert session.verify == verify


def test_cancel_group_marks_every_statement_and_cancels_only_its_running_ones():
    session = _RunningSession([])
    connection, queries = _tagging_connection(session)
    cursor = connection.cursor()
    group = "ab" * 16
    cursor.cancel_group = group
    cursor.execute("SELECT 1")
    first = cursor.query_tag
    cursor.execute("SELECT 2")
    second = cursor.query_tag
    assert queries[-2:] == [f"/* sqlalchemy-drill:{first} group:{group} */ SELECT 1",
                            f"/* sqlalchemy-drill:{second} group:{group} */ SELECT 2"]
    session.running = [
        {"queryId": "mine", "query": f"/* sqlalchemy-drill:{second} group:{group} */ SELECT 2"},
        {"queryId": "other-group", "query": f"/* sqlalchemy-drill:{'1' * 32} group:{'cd' * 16} */ SELECT 2"},
        {"queryId": "untagged", "query": f"SELECT 'group:{group} */'"},
    ]
    session.gets.clear()
    assert connection.cancel_query_group(group) is True
    assert [url for url, _ in session.gets if "/cancel/" in url] == [
        "http://h:8047/profiles/cancel/mine"]
    # The per-statement cancel still finds a grouped statement by its own tag.
    session.gets.clear()
    assert cursor.cancel() is True
    assert [url for url, _ in session.gets if "/cancel/" in url] == [
        "http://h:8047/profiles/cancel/mine"]


@pytest.mark.parametrize("bad", ["", "x" * 32, "AB" * 16, "ab", "ab" * 16 + " */ DROP"])
def test_cancel_group_must_be_a_plain_hex_id(bad):
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    session = _RunningSession([])
    connection, _ = _tagging_connection(session)
    cursor = connection.cursor()
    cursor.cancel_group = bad
    if bad:
        with pytest.raises(_drilldbapi.ProgrammingError, match="cancel_group"):
            cursor.execute("SELECT 1")
    with pytest.raises(_drilldbapi.ProgrammingError, match="cancel_group"):
        connection.cancel_query_group(bad)


def test_cancel_group_with_nothing_running_returns_false(monkeypatch):
    from sqlalchemy_drill.drilldbapi import _drilldbapi

    monkeypatch.setattr(_drilldbapi, "sleep", lambda _s: None)
    session = _RunningSession([])
    connection, _ = _tagging_connection(session)
    assert connection.cancel_query_group("ef" * 16) is False
    assert not any("/cancel/" in url for url, _ in session.gets)


def _rest_connection_returning(columns, metadata, rows):
    import io
    import json

    import requests

    from sqlalchemy_drill.drilldbapi import _drilldbapi

    class Session(_RecordingSession):
        def post(self, url, **kwargs):
            response = requests.Response()
            response.status_code = 200
            if "sys.drillbits" in kwargs["data"]:
                body = {"columns": ["version"], "metadata": ["VARCHAR"],
                        "rows": [{"version": "1.21.2"}], "queryState": "COMPLETED"}
            else:
                body = {"queryId": "q", "columns": columns, "metadata": metadata,
                        "rows": rows, "queryState": "COMPLETED"}
            response.raw = io.BytesIO(json.dumps(body).encode())
            return response

    return _drilldbapi.Connection("h", 8047, "http://", None, Session())


def test_rest_float_columns_decode_to_python_floats():
    # Drill sends DOUBLE/FLOAT values as JSON numbers, and NaN and the
    # infinities as the strings "NaN", "Infinity" and "-Infinity". The
    # description says FLOAT, so every value must be a Python float (or None),
    # never a Decimal or a string.
    import math
    import json as _json

    raw = ('{"queryId": "q", "columns": ["a", "b", "c", "d", "e"], '
           '"metadata": ["FLOAT8", "FLOAT4", "FLOAT8", "FLOAT8", "FLOAT8"], "rows": ['
           '{"a": 1.25, "b": 1.5, "c": "NaN", "d": "Infinity", "e": "-Infinity"},'
           '{"a": 0.1, "b": 0, "c": 1.0E300, "d": null, "e": -2}], '
           '"queryState": "COMPLETED"}')
    connection = _rest_connection_returning(**{k: v for k, v in _json.loads(raw).items()
                                                if k in ("columns", "metadata", "rows")})
    cursor = connection.cursor()
    cursor.execute("SELECT a, b, c, d, e FROM t")
    first, second = cursor.fetchall()
    assert [type(v) for v in first] == [float] * 5
    assert first[:2] == (1.25, 1.5) and math.isnan(first[2])
    assert first[3:] == (math.inf, -math.inf)
    assert second == (0.1, 0.0, 1e300, None, -2.0)
    assert [type(v) for v in second] == [float, float, float, type(None), float]


def test_rest_decimal_description_carries_precision_and_scale():
    import decimal

    connection = _rest_connection_returning(
        ["amount", "n", "name"], ["VARDECIMAL(12, 3)", "DECIMAL(38,0)", "VARCHAR(65535)"],
        [{"amount": 12345.678, "n": 12345678901234567890123456789012345678, "name": "x"}])
    cursor = connection.cursor()
    cursor.execute("SELECT amount, n, name FROM t")
    assert [(d[0], d[4], d[5]) for d in cursor.description] == [
        ("amount", 12, 3), ("n", 38, 0), ("name", None, None)]
    assert cursor.fetchall() == [(decimal.Decimal("12345.678"),
                                  decimal.Decimal("12345678901234567890123456789012345678"), "x")]


def test_file_reflection_maps_vardecimal_with_precision_and_scale(streaming_rest_engine):
    # Drill reports DECIMAL columns of file-backed tables (Parquet, JSON with
    # decimals enabled) as VARDECIMAL(p, s) in the probe's metadata.
    from sqlalchemy import types as sa_types

    engine, state = streaming_rest_engine
    state.probe, state.query_state = "columns", "COMPLETED"
    state.probe_columns = ["amount", "ratio", "n"]
    state.probe_metadata = ["VARDECIMAL(12, 3)", "FLOAT8", "BIGINT"]
    state.rows = [{"amount": 1.5, "ratio": 2.0, "n": 1}]
    with engine.connect() as connection:
        columns = inspect(connection).get_columns("t.parquet", "cp.default")
    amount, ratio, n = (column["type"] for column in columns)
    assert isinstance(amount, sa_types.DECIMAL)
    assert (amount.precision, amount.scale) == (12, 3)
    assert isinstance(ratio, sa_types.FLOAT) and isinstance(n, sa_types.BIGINT)
