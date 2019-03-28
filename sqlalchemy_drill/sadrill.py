# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
from sqlalchemy import exc, pool, types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from sqlalchemy import inspect
import requests
from pprint import pprint
from .base import DrillDialect, DrillIdentifierPreparer, DrillCompiler_sadrill, _type_map

try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler


try:
    from sqlalchemy.types import BigInteger
except ImportError:
    from sqlalchemy.databases.mysql import MSBigInteger as BigInteger

class DrillDialect_sadrill(DrillDialect):

    name = 'drilldbapi'
    driver = 'rest'
    dbapi = ""
    preparer = DrillIdentifierPreparer
    statement_compiler = DrillCompiler_sadrill
    poolclass = pool.SingletonThreadPool
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    def __init__(self, **kw):
        default.DefaultDialect.__init__(self, **kw)
        self.supported_extensions = []

    @classmethod
    def dbapi(cls):
        import sqlalchemy_drill.drilldbapi as module
        return module

    def create_connect_args(self, url, **kwargs):
        url_port = url.port or 8047
        qargs = {'host': url.host, 'port': url_port}

        try:
            db_parts = (url.database or 'drill').split('/')
            db = ".".join(db_parts)

            # Save this for later use.
            self.host = url.host
            self.port = url_port
            self.username = url.username
            self.password = url.password
            self.db = db


            # Get Storage Plugin Info:
            if db_parts[0]:
                self.storage_plugin = db_parts[0]

            if len(db_parts) > 1:
                self.workspace = db_parts[1]

            qargs.update(url.query)
            qargs['db'] = db
            if url.username:
                qargs['drilluser'] = url.username
                qargs['drillpass'] = ""
                if url.password:
                    qargs['drillpass'] = url.password
        except Exception as ex:
            print("************************************")
            print("Error in DrillDialect_sadrill.create_connect_args :: ", str(ex))
            print("************************************")
        return [], qargs
