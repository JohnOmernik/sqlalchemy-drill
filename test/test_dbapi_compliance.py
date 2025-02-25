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

import pytest

from sqlalchemy_drill.drilldbapi import _drilldbapi

from . import dbapi20


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
        "drilluser": "dbapi",
        "drillpass": "foo"
    }


@pytest.mark.usefixtures("drill_container_cls")
class DrillTest(dbapi20.DatabaseAPI20Test):

    driver = _drilldbapi

    def setUp(self):
        dbapi20.DatabaseAPI20Test.setUp(self)

    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)

    @pytest.mark.skip("No implemented")
    def test_ExceptionsAsConnectionAttributes(self):
        pass

    def test_description(self):
        """
        Overrides the base test to reflect that Drill does return row data for CREATE statements
        (each row reporting the number of records written by a writer fragment).
        """
        try:
            con = self._connect()
            cur = con.cursor()
            cur.execute(self.ddl1)
            self.assertEqual(
                len(cur.description),
                2,
                "cursor.description should describe two columns",
            )
            cur.execute("select name from %sbooze" % self.table_prefix)
            self.assertEqual(
                len(cur.description), 1, "cursor.description describes too many columns"
            )
            self.assertEqual(
                len(cur.description[0]),
                7,
                "cursor.description[x] tuples must have 7 elements",
            )
            self.assertEqual(
                cur.description[0][0].lower(),
                "name",
                "cursor.description[x][0] must return column name",
            )
            self.assertEqual(
                cur.description[0][1],
                self.driver.STRING,
                "cursor.description[x][1] must return column type. Got %r"
                % cur.description[0][1],
            )
        finally:
            con.close()

    def test_execute(self):
        """
        Overrides the base test with a simplified test of parameterised query execution.
        """
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute("select ? * ? as product", (3, 3))
            res = cur.fetchall()
            self.assertEqual([(9,)], res)
        finally:
            con.close()

    def test_executemany(self):
        """
        Overrides the base test with a simplified test of multiple parameterised query executions.
        """
        con = self._connect()
        try:
            cur = con.cursor()
            cur.executemany(
                f"create table `{self.table_prefix}_executemany/?_squared` as select ? * ? as product",
                [(i, i, i) for i in range(1, 4)],
            )

            cur.execute(
                f"select product from {self.table_prefix}_executemany order by product"
            )
            res = cur.fetchall()
            self.assertEqual([(1,), (4,), (9,)], res)
        finally:
            cur.execute(f"drop table if exists {self.table_prefix}_executemany")
            con.close()

    @pytest.mark.skip("Not implemented")
    def test_callproc(self):
        pass

    @pytest.mark.skip("Not implemented")
    def test_nextset(self):
        pass

    @pytest.mark.skip("Not implemented")
    def test_setoutputsize(self):
        pass
