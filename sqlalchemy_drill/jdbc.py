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
import jpype
import jpype.dbapi2 as jaydebeapi
import os
import logging
from .base import DrillDialect, DrillCompiler_sadrill


class DrillDialect_jdbc(DrillDialect):
    jdbc_db_name = "drill"
    jdbc_driver_name = "org.apache.drill.jdbc.Driver"

    statement_compiler = DrillCompiler_sadrill
    logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

    def __init__(self, *args, **kwargs):
        super(DrillDialect_jdbc, self).__init__(*args, **kwargs)
        self.jdbc_driver_path = os.environ.get('DRILL_JDBC_DRIVER_PATH')
        self.jdbc_jar_name = os.environ.get('DRILL_JDBC_JAR_NAME')

        if self.jdbc_driver_path is None:
            raise Exception('To connect to Drill via JDBC, you must set the DRILL_JDBC_DRIVER path to the location of the Drill JDBC driver.')

        if self.jdbc_jar_name is None:
            raise Exception(
                'To connect to Drill via JDBC, you must set the DRILL_JDBC_JAR_NAME environment variable.')
        print("-Djava.class.path=" + self.jdbc_driver_path + self.jdbc_jar_name)


    def initialize(self, connection):
        super(DrillDialect_jdbc, self).initialize(connection)

    """
    Open a connection to a database using a JDBC driver and return
        a Connection instance.
        jclassname: Full qualified Java class name of the JDBC driver.
        url: Database url as required by the JDBC driver.
        driver_args: Dictionary or sequence of arguments to be passed to
               the Java DriverManager.getConnection method. Usually
               sequence of username and password for the db. Alternatively
               a dictionary of connection arguments (where `user` and
               `password` would probably be included). See
               http://docs.oracle.com/javase/7/docs/api/java/sql/DriverManager.html
               for more details
        jars: Jar filename or sequence of filenames for the JDBC driver
        libs: Dll/so filenames or sequence of dlls/sos used as shared
              library by the JDBC driver
        """
    def create_connect_args(self, url):
        if url is not None:
            params = super(DrillDialect, self).create_connect_args(url)[1]
            driver = self.jdbc_driver_path + self.jdbc_jar_name

            cargs = (self.jdbc_driver_name,
                     self._create_jdbc_url(url),
                     [params['username'], params['password']],
                     driver)
            cparams = {p: params[p] for p in params if p not in ['host', 'username', 'password', 'port']}

            jpype.startJVM("-ea","-Djava.class.path=" + self.jdbc_driver_path + self.jdbc_jar_name)
            #dbapi2.connect('jdbc:drill:drillbit=localhost:31010', driver="org.apache.drill.jdbc.Driver")

            logging.info("Cargs:" + str(cargs))
            logging.info("Cparams" + str(cparams))

            return (cargs, cparams)

    def _create_jdbc_url(self, url):
        return "jdbc:drill:drillbit=%s:%s" % (
            url.host,
            url.port or 31010
        )

    @classmethod
    def dbapi(cls):
        return jaydebeapi

dialect = DrillDialect_jdbc