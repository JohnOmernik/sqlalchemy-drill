# -*- coding: utf-8 -*-
"""
Created on Thu Dec  1 08:58:12 2016

@author: cgivre
"""
from pydrill.dbapi import drill
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler
from .base import DrillExecutionContext, DrillDialect, DrillIdentifierPreparer, DrillCompiler


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
    storage_plugin = ""
    workspace = ""
    drill_connection = None    
    drill_cursor = None
    

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
        
        #Save this for later.
        self.host = url.host
        self.port = url.port
        self.username = url.username
        
        if len(db_parts) == 1:
            kwargs['catalog'] = db_parts[0]
            self.storage_plugin = db_parts[0]
        elif len(db_parts) == 2:
            kwargs['catalog'] = db_parts[0]
            kwargs['schema'] = db_parts[1]
            self.storage_plugin = db_parts[0]
            self.workspace = db_parts[1]
            
        else:
            raise ValueError("Unexpected database format {}".format(url.database))
        
        return ([], kwargs)

    def connect(self, *cargs, **cparams):
        connection = self.dbapi.connect(autocommit=True, *cargs, **cparams)
        self.drill_connection = connection
        self.drill_cursor = self.drill_connection.cursor()
       
        return connection


    def get_table_names(self, connection, schema=None, **kw):
        location = ""
        if( len(self.workspace) > 0 ):
            location = self.storage_plugin + "." + self.workspace
        else:
            location = self.storage_plugin
        
        from pydrill.client import PyDrill
        drill = PyDrill(host=self.host, port=self.port)

        file_dict = drill.query( "SHOW FILES IN " + location )
       
        temp = []
        for row in file_dict:
            temp.append( row['name'])
        
        table_names = tuple( temp )        
        '''        
        result = self.drill_cursor.fetchall()
        print( result )        
        table_names = [r[0] for r in result]
        return table_names
        '''
        return table_names
        
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

class DrillExecutionContext_pydrill(DrillExecutionContext):
    pass
