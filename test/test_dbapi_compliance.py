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

import dbapi20
import pytest

from sqlalchemy_drill.drilldbapi import _drilldbapi


@pytest.fixture(scope="class")
def drill_container_cls(request, drill_container):
    """
    A class-scoped fixture that exists solely to inject the session scoped fixture
    'drill_container', and configuration resulting from it, into the unittest class below.
    """
    request.cls.connect_kw_args = {
        "host": drill_container.get_container_host_ip(),
        "port": drill_container.get_exposed_port(8047),
        "db": "dfs.tmp",
    }


@pytest.mark.usefixtures("drill_container_cls")
class DrillTest(dbapi20.DatabaseAPI20Test):

    driver = _drilldbapi
    # lower_func = "lower"  # For stored procedure test

    def setUp(self):
        dbapi20.DatabaseAPI20Test.setUp(self)

    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)

    def test_non_idempotent_close(self):
        pass

    def test_nextset(self):
        pass

    def test_setoutputsize(self):
        pass
