import datetime
import decimal
import importlib
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
        }
        self.tables = {
            ("jdbc.prod", "accounts"),
            ("jdbc.prod", "accounts.view.drill"),
        }
        self.views = {
            ("dfs.tmp", "saved_view"),
            ("jdbc.prod", "account_view"),
        }
        self.columns = {
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

    with engine.connect() as connection:
        dialect = connection.dialect
        assert dialect.get_plugin_type(connection, malicious_schema) is None
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
        query_state="FAILED", rows=[{"v": "1"}], cursors=[], calls=[]
    )

    def post(_url, *, data, **_kwargs):
        query = json.loads(data)["query"]
        state.calls.append(query)
        columns, metadata, rows = ["v"], ["INTEGER"], state.rows
        query_state = "COMPLETED"
        if "sys.drillbits" in query:
            columns, metadata = ["version"], ["VARCHAR"]
            rows = [{"version": "1.21.2"}]
        elif "INFORMATION_SCHEMA.`SCHEMATA`" in query:
            columns, metadata = ["SCHEMA_NAME", "TYPE"], ["VARCHAR"] * 2
            plugin_type = "jdbc" if state.probe == "exists" else "file"
            rows = [{"SCHEMA_NAME": "cp.default", "TYPE": plugin_type}]
        elif "SHOW FILES" in query or "INFORMATION_SCHEMA.`VIEWS`" in query:
            rows = []
        elif query.startswith("SELECT 1") and state.probe == "fallback":
            rows = []
        else:
            query_state = state.query_state
        # The real Requests response and REST cursor parse rows before the
        # opaque trailing state, matching StreamingHttpConnection.finish().
        response = requests.Response()
        response.status_code = 200
        response.raw = io.BytesIO(json.dumps({
            "columns": columns, "metadata": metadata, "rows": rows,
            "queryState": query_state,
        }).encode())
        return response

    original_cursor = Connection.cursor

    def cursor(connection):
        result = original_cursor(connection)
        state.cursors.append(result)
        return result

    monkeypatch.setattr(Connection, "cursor", cursor)
    session = requests.Session()
    monkeypatch.setattr(session, "post", post)
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
