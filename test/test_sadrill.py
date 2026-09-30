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


@pytest.fixture(scope="module", params=[False, True], ids=["default", "verbose"])
def drill_conn(drill_container, request):
    drill_ip = drill_container.get_container_host_ip()
    drill_port = drill_container.get_exposed_port(8047)
    # testcontainer credentials are not sensitive and intentionally hard coded.
    engine = create_engine(f"drill+sadrill://dbapi:foo@{drill_ip}:{drill_port}/dfs.tmp")

    # Drill's process user is the administrator; reflection still runs as the
    # non-admin dbapi user, including every permission-denied assertion.
    admin_engine = create_engine(engine.url.set(username="drilluser"))
    try:
        with admin_engine.connect() as admin, engine.connect() as drill_conn:
            option = "drill.exec.http.rest.errors.verbose"
            try:
                admin.exec_driver_sql(
                    f"ALTER SYSTEM SET `{option}` = {str(request.param).lower()}"
                ).fetchall()
                assert drill_conn.exec_driver_sql(
                    f"SELECT val FROM sys.options WHERE name = '{option}'"
                ).scalar() == str(request.param).lower()
                yield drill_conn
            finally:
                admin.exec_driver_sql(f"ALTER SYSTEM RESET `{option}`").fetchall()
                assert drill_conn.exec_driver_sql(
                    f"SELECT val FROM sys.options WHERE name = '{option}'"
                ).scalar() == "false"
    finally:
        admin_engine.dispose()
        engine.dispose()


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
    # Classpath resources have no authoritative directory listing.
    with pytest.raises(sa_exc.DatabaseError):
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


def test_missing_file_autoload_raises_no_such_table(drill_conn):
    with pytest.raises(sa_exc.NoSuchTableError):
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
        "a%b",
        "naïve",
    ):
        # These remain single missing identifiers, never executable SQL.
        assert not inspector.has_table(table_name, schema="dfs.tmp")


@pytest.fixture(scope="module")
def absence_files(drill_container):
    container = drill_container.get_wrapped_container()
    # A separate directory in the disposable integration container, never on
    # the host. Run setup as root so even the file owner differs from Drill.
    setup = container.exec_run(["sh", "-c", """
        mkdir -p /tmp/sa_absence/readable /tmp/sa_absence/empty /tmp/sa_absence/denied
        printf '{"v":1}\n' > /tmp/sa_absence/readable/data.json
        cp /tmp/sa_absence/readable/data.json /tmp/sa_absence/denied/data.json
        cp /tmp/sa_absence/readable/data.json /tmp/sa_absence/readable/denied.json
        chmod 755 /tmp/sa_absence /tmp/sa_absence/readable /tmp/sa_absence/empty
        chmod 644 /tmp/sa_absence/readable/data.json /tmp/sa_absence/denied/data.json
        chmod 000 /tmp/sa_absence/denied /tmp/sa_absence/readable/denied.json
    """], user="root")
    assert setup.exit_code == 0, setup.output
    try:
        yield container
    finally:
        cleanup = container.exec_run(["rm", "-rf", "/tmp/sa_absence"], user="root")
        assert cleanup.exit_code == 0, cleanup.output


def _live_reflect(connection, operation, name):
    if operation == "autoload":
        return Table(name, MetaData(), schema="dfs.tmp", autoload_with=connection)
    return getattr(inspect(connection), operation)(name, schema="dfs.tmp")


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
def test_syntax_error_during_reflection_raises_live(drill_conn, operation):
    from sqlalchemy import event

    def invalid_probe(connection, cursor, statement, parameters, context, executemany):
        if statement.startswith("SELECT *"):
            return "SELECT FROM", parameters
        return statement, parameters

    event.listen(drill_conn, "before_cursor_execute", invalid_probe, retval=True)
    try:
        with pytest.raises(sa_exc.DatabaseError):
            _live_reflect(drill_conn, operation, "syntax_probe.json")
    finally:
        event.remove(drill_conn, "before_cursor_execute", invalid_probe)


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
def test_unreachable_server_raises_live(drill_conn, operation):
    import socket
    from requests import ConnectionError

    from sqlalchemy_drill.drilldbapi import TransportError

    # Bind without listening: a real refused TCP connection, without racing
    # another process to claim a supposedly unused port. A refused transport
    # is a disconnect, and SQLAlchemy invalidates that connection, so use a
    # dedicated one rather than the module-scoped drill_conn.
    engine = create_engine(drill_conn.engine.url)
    try:
        with socket.socket() as unavailable:
            unavailable.bind(("127.0.0.1", 0))
            with engine.connect() as conn:
                conn.connection.dbapi_connection._base_url = (
                    f"http://127.0.0.1:{unavailable.getsockname()[1]}")
                with pytest.raises(sa_exc.OperationalError) as caught:
                    _live_reflect(conn, operation, "unreachable.json")
        # Never classified as absence: a DB-API disconnect caused by the
        # refused connection.
        assert isinstance(caught.value.orig, TransportError)
        assert isinstance(caught.value.orig.__cause__, ConnectionError)
        assert caught.value.connection_invalidated
    finally:
        engine.dispose()


def test_success_never_fetches_profile_live(drill_conn, absence_files, monkeypatch):
    def unexpected_get(*args, **kwargs):
        pytest.fail("Successful reflection must not fetch a query profile")

    monkeypatch.setattr(drill_conn.connection.dbapi_connection._session,
                        "get", unexpected_get)
    for operation in ("has_table", "get_columns", "autoload"):
        result = _live_reflect(drill_conn, operation, "sa_absence/readable/data.json")
        if operation == "has_table":
            assert result is True
        elif operation == "get_columns":
            assert result
        else:
            assert len(result.columns) > 0


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
def test_readable_directory_typo_is_proven_absent(drill_conn, absence_files, operation):
    name = "sa_absence/readable/typo.json"
    if operation == "has_table":
        assert _live_reflect(drill_conn, operation, name) is False
    else:
        with pytest.raises(sa_exc.NoSuchTableError):
            _live_reflect(drill_conn, operation, name)


@pytest.mark.parametrize("operation", ["has_table", "get_columns", "autoload"])
@pytest.mark.parametrize("name", [
    "sa_absence/denied/data.json", "sa_absence/readable/denied.json",
    "sa_absence/empty/typo.json", "a?b",
])
def test_unproven_or_denied_file_raises_live(drill_conn, absence_files, operation, name):
    with pytest.raises(sa_exc.DatabaseError) as caught:
        _live_reflect(drill_conn, operation, name)
    assert caught.value.orig._drill_cursor.result_md["queryState"] == "FAILED"


def test_listing_metadata_does_not_identify_filesystem_user(drill_conn, absence_files):
    entries = drill_conn.exec_driver_sql(
        "SHOW FILES FROM dfs.tmp.sa_absence"
    ).mappings().all()
    directories = {entry["name"]: entry for entry in entries}
    assert directories["denied"]["permissions"] == "---------"
    assert directories["empty"]["permissions"] == "rwxr-xr-x"
    assert directories["readable"]["owner"] == "root"
    assert directories["readable"]["group"] == "root"
    assert directories["readable"]["isDirectory"] is True
    files = drill_conn.exec_driver_sql(
        "SELECT * FROM INFORMATION_SCHEMA.`FILES` "
        "WHERE SCHEMA_NAME = 'dfs.tmp' AND FILE_NAME = 'sa_absence'"
    ).mappings().all()
    assert len(files) == 1
    assert files[0]["IS_DIRECTORY"] is True
    assert files[0]["OWNER"] == files[0]["GROUP"] == "root"
    assert files[0]["PERMISSION"] == "rwxr-xr-x"
    assert drill_conn.exec_driver_sql(
        "SELECT bool_val FROM sys.boot WHERE name = 'drill.exec.impersonation.enabled'"
    ).fetchall() == [(False,)]
    assert drill_conn.exec_driver_sql(
        "SELECT SESSION_USER FROM (values(1))"
    ).fetchall() == [("dbapi",)]
    identity = absence_files.exec_run(["id", "-un"])
    assert identity.exit_code == 0
    assert identity.output.strip() == b"drilluser"
    # Reading a root-owned file demonstrably does NOT make Drill root.
    assert drill_conn.exec_driver_sql(
        "SELECT v FROM dfs.tmp.`sa_absence/readable/data.json`"
    ).fetchall() == [(1,)]
    assert inspect(drill_conn).has_table("sa_absence/readable/data.json", schema="dfs.tmp")


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
