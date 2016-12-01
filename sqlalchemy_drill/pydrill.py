# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 08:58:12 2016

@author: cgivre
"""
from pydrill.dbapi import drill
from pydrill.dbapi.common import UniversalSet
from sqlalchemy import exc
from sqlalchemy import types
from sqlalchemy import util
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from .base import DrillExecutionContext, DrillDialect
import decimal
import re


try:
    from sqlalchemy.sql.compiler import SQLCompiler
except ImportError:
    from sqlalchemy.sql.compiler import DefaultCompiler as SQLCompiler

class DrillDialect_pydrill(default.DefaultDialect):
    name = 'drill'
    driver = 'rest'
    preparer = DrillIdentifierPreparer
    statement_compiler = DrillCompiler
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    @classmethod
    def dbapi(cls):
        return drill

    def create_connect_args(self, url):
        db_parts = (url.database or 'drill').split('/')
        kwargs = {
            'host': url.host,
            'port': url.port or 8047,
            'username': url.username,
        }
        kwargs.update(url.query)
        if len(db_parts) == 1:
            kwargs['catalog'] = db_parts[0]
        elif len(db_parts) == 2:
            kwargs['catalog'] = db_parts[0]
            kwargs['schema'] = db_parts[1]
        else:
            raise ValueError("Unexpected database format {}".format(url.database))
        return ([], kwargs)


    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        # Drill has no support for primary keys.
        return []

    def get_indexes(self, connection, table_name, schema=None, **kw):
        #Drill has no support for indexes.        
        return []

    def do_rollback(self, dbapi_connection):
        # No transactions for Presto
        pass

    def _check_unicode_returns(self, connection, additional_tests=None):
        # requests gives back Unicode strings
        return True

    def _check_unicode_description(self, connection):
        # requests gives back Unicode strings
        return True
