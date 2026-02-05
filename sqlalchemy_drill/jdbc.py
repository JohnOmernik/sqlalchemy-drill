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

from __future__ import absolute_import
from __future__ import unicode_literals
import logging

import jpype
from jpype import dbapi2

from .base import DrillDialect, DrillCompiler_sadrill

logger = logging.getLogger(__name__)


class JDBCError(Exception):
    """Exception raised for JDBC-related errors."""


class DrillDialect_jdbc(DrillDialect):
    """
    Drill dialect using JDBC driver.

    Open a connection to a database using a JDBC driver and return
    a Connection instance.

    The user is responsible for starting the JVM with the class path.
    """

    jdbc_db_name = "drill"
    jdbc_driver_name = "org.apache.drill.jdbc.Driver"
    statement_compiler = DrillCompiler_sadrill

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # The user is responsible for starting the JVM with the class path, but we can
        # do some sanity checks to make sure they are pointed in the right direction.
        if not jpype.isJVMStarted():
            raise JDBCError("The JVM must be started before connecting to a JDBC driver.")
        try:
            jpype.JClass("org.apache.drill.jdbc.Driver")
        except TypeError as exc:
            classpath = str(jpype.java.lang.System.getProperty('java.class.path'))
            raise JDBCError(
                f"The drill JDBC driver class was not located in the CLASSPATH `{classpath}`"
            ) from exc

    def create_connect_args(self, url, **kwargs):
        """
        Create connection arguments for JDBC.

        Args:
            url: Database url as required by the JDBC driver.
            **kwargs: Additional connection arguments including:
                - driver_args: Dictionary or sequence of arguments to be passed to
                  the Java DriverManager.getConnection method. Usually sequence of
                  username and password for the db.

        Returns:
            Tuple of (args, kwargs) for the connection.
        """
        if url is None:
            return [], {}

        params = super(DrillDialect, self).create_connect_args(url)[1]

        # We only need the dsn url as an argument
        cargs = (self._create_jdbc_url(url),)

        # Everything else is passed as keywords
        cparams = {p: params[p] for p in params if p not in ['host', 'port']}

        logger.info(f"Cargs: {cargs}")
        logger.info(f"Cparams: {cparams}")

        return cargs, cparams

    def _create_jdbc_url(self, url):
        return f"jdbc:drill:drillbit={url.host}:{url.port or 31010}"

    @classmethod
    def dbapi(cls):
        return dbapi2


dialect = DrillDialect_jdbc
