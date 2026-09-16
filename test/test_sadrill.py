import datetime

import pytest
from sqlalchemy import (
    Column,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
    select,
    text,
)
from sqlalchemy import exc as sa_exc


@pytest.fixture(scope="module")
def drill_conn(drill_container):
    drill_ip = drill_container.get_container_host_ip()
    drill_port = drill_container.get_exposed_port(8047)
    # testcontainer credentials are not sensitive and intentionally hard coded.
    engine = create_engine(f"drill+sadrill://dbapi:foo@{drill_ip}:{drill_port}/dfs.tmp")

    with engine.connect() as drill_conn:
        yield drill_conn


def test_empty_result_set(drill_conn):
    res = drill_conn.exec_driver_sql("SELECT CURRENT_TIMESTAMP LIMIT 0")

    assert [] == list(res)


def test_rest_query_and_reflection(drill_conn):
    rows = drill_conn.exec_driver_sql(
        "SELECT employee_id, first_name "
        "FROM cp.`employee.json` ORDER BY employee_id LIMIT 2"
    ).fetchall()
    assert rows == [(1, "Sheri"), (2, "Derrick")]

    inspector = inspect(drill_conn)
    assert "options" in inspector.get_table_names(schema="sys")
    assert [
        column["name"]
        for column in inspector.get_columns("options", schema="sys")
    ] == [
        "name",
        "kind",
        "accessibleScopes",
        "val",
        "status",
        "optionScope",
        "description",
    ]
    assert inspector.has_table("employee.json", schema="cp.default")
    # With verbose errors disabled, absence is indistinguishable from other
    # server failures. Reflection must preserve that error, not report False.
    with pytest.raises(sa_exc.DBAPIError):
        inspector.has_table("missing.json", schema="cp.default")


def test_schema_qualified_select_executes(drill_conn):
    # Drill accepts only a one-part table qualifier on a column reference, so
    # SQLAlchemy's default "schema.table.column" rendering fails validation
    # with "Table 'cp' not found".  Compile and run it for real.
    employees = Table(
        "employee.json",
        MetaData(),
        Column("employee_id", Integer),
        Column("first_name", String),
        schema="cp",
    )
    statement = select(employees.c.employee_id, employees.c.first_name).where(
        employees.c.employee_id == 1
    )

    compiled = str(statement.compile(bind=drill_conn.engine))
    assert "FROM cp.`employee.json`" in compiled
    assert "cp.`employee.json`.employee_id" not in compiled

    assert drill_conn.execute(statement).fetchall() == [(1, "Sheri")]


def test_bare_plugin_schema_reflects(drill_conn):
    # INFORMATION_SCHEMA.SCHEMATA has no row for a bare plugin name, only for
    # its workspaces, so an exact-match lookup resolved "dfs" to no plugin
    # type and reflected it as empty.
    inspector = inspect(drill_conn)
    assert drill_conn.dialect.get_plugin_type(drill_conn, "dfs") == "file"
    assert drill_conn.dialect.get_plugin_type(drill_conn, "dfs.tmp") == "file"
    assert drill_conn.dialect.get_plugin_type(drill_conn, "sys") == "system-tables"
    assert drill_conn.dialect.get_plugin_type(drill_conn, "nosuchplugin") is None
    # Resolving the plugin type is what routes "dfs" to SHOW FILES; while it
    # resolved to None this came back empty.
    assert inspector.get_table_names(schema="dfs")


def test_opaque_missing_file_failure_remains_a_dbapi_error(drill_conn):
    with pytest.raises(sa_exc.DBAPIError):
        Table(
            "definitely_missing.json",
            MetaData(),
            autoload_with=drill_conn,
            schema="dfs.tmp",
        )


@pytest.mark.parametrize(
    "value",
    [
        "plain",
        "x' OR '1'='1",
        "has a ? question mark",
        "trailing backslash \\",
        "quote ' and backtick `",
        "; DROP TABLE x; --",
        "*/ 999 AS pwned FROM (values(1)) -- ",
        "naïve 表",
    ],
)
def test_parameters_round_trip_as_data_not_sql(drill_conn, value):
    # Every one of these must come back byte for byte, and none may change the
    # shape of the result set.
    result = drill_conn.execute(
        text("SELECT :value AS v FROM (values(1))"), {"value": value}
    )
    assert list(result.keys()) == ["v"]
    assert result.fetchall() == [(value,)]


def test_datetime_parameter_casts_to_timestamp(drill_conn):
    # Drill requires 'yyyy-MM-dd HH:mm:ss'; an ISO 'T' separator raises
    # DateTimeParseException.
    moment = datetime.datetime(2020, 1, 2, 3, 4, 5)
    rows = drill_conn.execute(
        text("SELECT CAST(:value AS TIMESTAMP) AS v FROM (values(1))"),
        {"value": moment},
    ).fetchall()
    assert rows == [(moment,)]


def test_block_comment_parameter_cannot_escape(drill_conn):
    # "/*/ ... */" is a single comment to Drill, so the template below has no
    # placeholders and supplying one is an error rather than an injection.
    from sqlalchemy_drill.drilldbapi import ProgrammingError

    template = "SELECT /*/ ? */ 2 AS v FROM (values(1))"
    with pytest.raises(ProgrammingError, match="Too many"):
        drill_conn.connection.dbapi_connection.cursor().execute(
            template, ("a*/ 999 AS pwned FROM (values(1)) -- ",)
        )

    rows = drill_conn.exec_driver_sql(template).fetchall()
    assert rows == [(2,)]


def test_injection_shaped_identifiers_stay_identifiers(drill_conn):
    inspector = inspect(drill_conn)
    for table_name in (
        "foo`; DROP TABLE x; --",
        "foo' OR '1'='1",
        "a?b",
        "a%b",
        "naïve",
    ):
        # The missing identifier must fail, without suppressing an opaque
        # server error as a successful absence check.
        with pytest.raises(sa_exc.DBAPIError):
            inspector.has_table(table_name, schema="dfs.tmp")


def test_readiness_retries_a_slow_starting_http_endpoint(monkeypatch):
    from types import SimpleNamespace
    from . import conftest

    replies = iter([conftest.requests.exceptions.ReadTimeout(), SimpleNamespace(status_code=200)])
    calls = []

    def get(url, timeout):
        calls.append((url, timeout))
        reply = next(replies)
        if isinstance(reply, Exception):
            raise reply
        return reply

    monkeypatch.setattr(conftest.requests, "get", get)
    monkeypatch.setattr(conftest.time, "sleep", lambda _seconds: None)
    container = SimpleNamespace(
        get_container_host_ip=lambda: "127.0.0.1",
        get_exposed_port=lambda _port: "8047",
    )
    conftest.wait_for_http_up(container)
    assert calls == [("http://127.0.0.1:8047", 2)] * 2
