"""Array regression replay; DRILL_REVIEW_URL can select an existing local server."""
import math
import os
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url


@pytest.fixture(params=['drill', 'drill+sadrill'])
def float_connection(request):
    url = os.environ.get('DRILL_REVIEW_URL')
    if not url:
        server = request.getfixturevalue('drill_container')
        url = ('drill+sadrill://dbapi:foo@%s:%s/dfs.tmp' %
               (server.get_container_host_ip(), server.get_exposed_port(8047)))
    engine = create_engine(make_url(url).set(drivername=request.param), future=True)
    try:
        with engine.connect() as connection:
            yield connection
    finally:
        engine.dispose()


def test_json_repeated_float(float_connection):
    result = float_connection.exec_driver_sql(
        "SELECT convert_from('[1.5, 2.5]', 'JSON') arr FROM (VALUES(1))")
    assert result.cursor.result_md['metadata'] == ['FLOAT8']
    rows = result.fetchall()
    assert rows == [([1.5, 2.5],)]
    assert all(type(item) is float for item in rows[0][0])


def test_parquet_repeated_float(float_connection):
    name = 'float_review_' + uuid.uuid4().hex
    table = 'dfs.tmp.`%s`' % name
    float_connection.exec_driver_sql(
        'CREATE TABLE %s AS SELECT ' % table +
        "convert_from('[1.5, 2.5]', 'JSON') arr FROM (VALUES(1))").fetchall()
    try:
        result = float_connection.exec_driver_sql('SELECT arr FROM ' + table)
        assert result.cursor.result_md['metadata'] == ['FLOAT8']
        rows = result.fetchall()
        assert rows == [([1.5, 2.5],)]
        assert all(type(item) is float for item in rows[0][0])
    finally:
        float_connection.exec_driver_sql('DROP TABLE ' + table).fetchall()


def test_scalar_float_and_decimal_unchanged(float_connection):
    row = float_connection.exec_driver_sql(
        "SELECT CAST(1.5 AS DOUBLE), CAST(1.1 AS FLOAT), "
        "CAST('NaN' AS DOUBLE), CAST('Infinity' AS DOUBLE), "
        "CAST('-Infinity' AS DOUBLE), CAST(NULL AS DOUBLE), "
        "CAST(12.345 AS DECIMAL(12,3)) FROM (VALUES(1))").one()
    assert row[0] == 1.5 and row[1] == pytest.approx(1.1)
    assert all(type(value) is float for value in row[:5])
    assert math.isnan(row[2]) and row[3:6] == (math.inf, -math.inf, None)
    assert row[6] == Decimal('12.345') and type(row[6]) is Decimal
