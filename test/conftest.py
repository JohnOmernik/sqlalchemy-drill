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

from pathlib import Path

import pytest
import requests
from sqlalchemy.dialects import registry
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_container_is_ready

registry.register("drill", "sqlalchemy_drill.pyodbc", "DrillDialect_pyodbc")
registry.register("drill.pyodbc", "sqlalchemy_drill.pyodbc", "DrillDialect_pyodbc")
registry.register("drill.pydrill", "sqlalchemy_drill.pydrill", "DrillDialect_pydrill")


@wait_container_is_ready(requests.exceptions.ConnectionError)
def wait_for_http_up(drill_container):
    drill_ip, drill_http_port = (
        drill_container.get_container_host_ip(),
        drill_container.get_exposed_port(8047),
    )
    status_code = requests.get(f"http://{drill_ip}:{drill_http_port}").status_code
    print(f"Received HTTP {status_code}")
    return requests.get(f"http://{drill_ip}:{drill_http_port}").status_code == 200


@pytest.fixture(scope="session")
def drill_container():
    test_dir = Path("test/").absolute()
    try:
        drill_container = DockerContainer("apache/drill:latest")
        drill_container.with_exposed_ports(8047)\
            .with_volume_mapping(test_dir/"drill-override.conf", "/opt/drill/conf/drill-override.conf")\
            .with_volume_mapping(test_dir/"htpasswd", "/opt/drill/conf/htpasswd")\
            .with_kwargs(entrypoint="/bin/bash")\
            .with_command(["-c", "$DRILL_HOME/bin/drill-embedded -n dbapi -p foo -f <(sleep infinity)"])\
            .start()

        wait_for_http_up(drill_container)
        yield drill_container
    finally:
        drill_container.stop()
