import pytest
from sqlalchemy import create_engine


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
