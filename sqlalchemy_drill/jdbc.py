# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
from sqlalchemy import exc, pool, types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler

from sqlalchemy import inspect
import requests
import jaydebeapi
import logging
from .base import DrillDialect, DrillIdentifierPreparer, DrillCompiler_sadrill


class DrillDialect_jdbc(DrillDialect):
    jdbc_db_name = "drill"
    jdbc_driver_name = "org.apache.drill.jdbc.Driver"

    statement_compiler = DrillCompiler_sadrill

    def __init__(self, *args, **kwargs):
        super(DrillDialect_jdbc, self).__init__(*args, **kwargs)

    def initialize(self, connection):
        super(DrillDialect_jdbc, self).initialize(connection)

    """Open a connection to a database using a JDBC driver and return
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

            # TODO Remove Hard Coded Driver
            drill_path = "/Users/cgivre/github/drill-dev/drill/distribution/target/apache-drill-1.16.0-SNAPSHOT/apache-drill-1.16.0-SNAPSHOT/jars/jdbc-driver/"
            driver_name = "drill-jdbc-all-1.16.0-SNAPSHOT.jar"
            driver = drill_path + driver_name

            cargs = (self.jdbc_driver_name,
                     self._create_jdbc_url(url),
                     [params['username'], params['password']],
                     driver)
            cparams = {p: params[p] for p in params if p not in ['host', 'username', 'password', 'port']}

            print("Cargs:", cargs)
            print("Cparams", cparams)

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