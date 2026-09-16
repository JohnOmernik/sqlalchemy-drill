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

import os
import time
from pathlib import Path

import pytest
import requests
from sqlalchemy.dialects import registry

try:
    from testcontainers.core.container import DockerContainer
except ImportError:  # Unit tests do not require Docker/testcontainers.
    DockerContainer = None

registry.register("drill", "sqlalchemy_drill.sadrill", "DrillDialect_sadrill")
registry.register("drill.sadrill", "sqlalchemy_drill.sadrill", "DrillDialect_sadrill")
registry.register("drill.pyodbc", "sqlalchemy_drill.pyodbc", "DrillDialect_pyodbc")
registry.register("drill.pydrill", "sqlalchemy_drill.pydrill", "DrillDialect_pydrill")


def wait_for_http_up(drill_container):
    drill_ip, drill_http_port = (
        drill_container.get_container_host_ip(),
        drill_container.get_exposed_port(8047),
    )
    url = f"http://{drill_ip}:{drill_http_port}"
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            status_code = requests.get(url, timeout=2).status_code
            if status_code == 200:
                print(f"Received HTTP {status_code}")
                return
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            pass
        time.sleep(1)
    raise RuntimeError(f"Apache Drill did not become ready at {url}")


@pytest.fixture(scope="session")
def drill_container():
    if os.environ.get("DRILL_RUN_REST_INTEGRATION") != "1":
        pytest.skip("set DRILL_RUN_REST_INTEGRATION=1 for Apache Drill tests")
    # Once the run has explicitly opted in, a missing dependency is a failure
    # rather than a skip: CI sets this variable precisely so that these tests
    # cannot pass by quietly not running.
    if DockerContainer is None:
        raise RuntimeError(
            "DRILL_RUN_REST_INTEGRATION=1 but testcontainers is not "
            "installed; install requirements/test.txt"
        )

    test_dir = Path(__file__).parent.absolute()
    drill_container = DockerContainer(
        "apache/drill:1.21.2@sha256:"
        "8d572b93e2676155acfbf426e8ea18e7ce3827c9fec8f3df81d1f88729cfa293"
    )
    drill_container.with_exposed_ports(8047)\
        .with_volume_mapping(test_dir/"drill-override.conf", "/opt/drill/conf/drill-override.conf")\
        .with_volume_mapping(test_dir/"htpasswd", "/opt/drill/conf/htpasswd")\
        .with_kwargs(entrypoint="/bin/bash")\
        .with_command(["-c", "$DRILL_HOME/bin/drill-embedded -n dbapi -p foo -f <(sleep infinity)"])

    # Construct before the try block: if starting Drill fails, the original
    # error must propagate rather than being replaced by an UnboundLocalError
    # from the cleanup path.
    drill_container.start()
    try:
        wait_for_http_up(drill_container)
        yield drill_container
    finally:
        drill_container.stop()
