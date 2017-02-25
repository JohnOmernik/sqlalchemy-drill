# -*- coding: utf-8 -*-
"""
Created on Mon Nov 28 11:02:44 2016

@author: cgivre
"""
from __future__ import absolute_import
from __future__ import unicode_literals
import re
import sqlparse
from sqlalchemy_drill.drilldbapi import common
from sqlalchemy_drill.drilldbapi.common import DBAPICursor as DBAPICursor
from sqlalchemy_drill.drilldbapi.exceptions import *
from pydrill.client import PyDrill
from collections import OrderedDict

# PEP 249 module globals
apilevel = '2.0'
threadsafety = 2  # Threads may share the module and connections.
paramstyle = 'pyformat'

def connect(*args, **kwargs):
    """Constructor for creating a connection to the database. See class :py:class:`Connection` for
    arguments.

    :returns: a :py:class:`Connection` object.
    """
    return Connection(*args, **kwargs)

class Connection(object):
    """Drill does not have a notion of a persistent connection.

    Thus, these objects are small stateless factories for cursors, which do all the real work.
    """
    def __init__(self, *args, **kwargs):
        self._kwargs = kwargs
        self._args = args
        self._conn = PyDrill(**kwargs)

    def close(self):
        """Closes active connection to Drill.  """
        self.drill = None

    def execute(self, q):
        """ Executes a query!"""
        print("######### in Connection.execute")
        return self._conn.query(q)


    def commit(self):
        """Drill does not support transactions, so this does nothing."""
        pass

    def rollback(self):
        """Drill does not support transactions, so this does nothing."""
        pass

    def cursor(self):
        """Return a new :py:class:`Cursor` object using the connection."""
        return Cursor(self._conn, **self._kwargs)


class Cursor(common.DBAPICursor):
    """These objects represent a database cursor, which is used to manage the context of a fetch
    operation.

    Cursors are not isolated, i.e., any changes done to the database by a cursor are immediately
    visible by other cursors or connections.
    """
    _data = []
    _description = None

    _FLOAT_FUNCTIONS = [
        'CBRT',
        'DEGREES',
        'E',
        'EXP',
        'LOG',
        'MOD',
        'PI',
        'POW',
        'RADIANS'
        'RAND',
        'ROUND',
        'TRUNC',
        'EXTRACT',
        'DATE_PART',
        'AVG',
        'CUME_DIST',
        'PERCENT_RANK'
    ]

    _INT_FUNCTIONS = [
        'SIGN',
        'CHAR_LENGTH',
        'POSITION',
        'STRPOS',
        'NTILE'
    ]
    _BIG_INT_FUNCTIONS = [
        'COUNT',
        'DENSE_RANK',
        'RANK',
        'ROW_NUMBER'

    ]

    def __init__(self, conn, poll_interval=1, **kwargs):
        """
        :param host: hostname to connect to, e.g. ``localhost``
        :param port: int -- port, defaults to 8047
        :param user: string -- defaults to system user name
        :param catalog: string -- defaults to ``drill``
        :param schema: string -- defaults to ``dfs``
        :param poll_interval: int -- how often to ask the Presto REST interface for a progress
            update, defaults to a second
        :param source: string -- arbitrary identifier (shows up in the Presto monitoring page)
        """
        print("Start cursor init")

        self._myconn = conn

        super(Cursor, self).__init__(poll_interval)
#        # Config
#        self._host = kwargs['host']
#        self._port = kwargs['port']
#        #self._username = username or getpass.getuser()
#        self._catalog = catalog
#        self._schema = schema
#        self._arraysize = 1
        self._poll_interval = poll_interval
#        self._source = source
#        self._session_props = session_props if session_props is not None else {}
        #self._autocommit = autocommit
 #       self._reset_state()
 #       self._rownumber = 0
 #       self._description = None
 #       self._columns = OrderedDict()
 #       self._actual_cols = None
 #       self._connectargs = {"host":host, "port": port}
        #self.paramstyle = "pyformat"

        #if drill_auth in kwargs:
        #    self._connectargs["drill_auth"] = drill_auth
        #if use_ssl in kwargs:
        #    self._connectargs["use_ssl"] = use_ssl
        #if verify_certs in kwargs:
        #    self._connectargs["verify_certs"] = verify_certs
        #if ca_certs in kwargs:
        #    self._connectargs["ca_certs"] = ca_certs
        print("****** Finish Cursor init")
 #       print(self._connectargs)

    def get_schema(self):
        return self._schema

    def _reset_state(self):
        """Reset state about the previous query in preparation for running another query"""
        super(Cursor, self)._reset_state()
        self._nextUri = None
        self._columns = OrderedDict()
        self._rownumber = 0
        self._operation = None
        self._actual_cols = OrderedDict()

    def _fetch_more(self):
        pass

    @property
    def description(self):
        """This read-only attribute is a sequence of 7-item sequences.

        Each of these sequences contains information describing one result column:

        - name
        - type_code
        - display_size (None in current implementation)
        - internal_size (None in current implementation)
        - precision (None in current implementation)
        - scale (None in current implementation)
        - null_ok (always True in current implementation)

        The ``type_code`` can be interpreted by comparing it to the Type Objects specified in the
        section below.
        """
        # Sleep until we're done or we got the columns
        self._fetch_while(
            lambda: self._columns is None and
            self._state not in (self._STATE_NONE, self._STATE_FINISHED)
        )
        if self._columns is None:
            return None

        #TODO Configure for various storage plugins

        showRegexObj = re.match(r'SHOW', self._operation, re.I )
        if showRegexObj:
            result = [
                # name, type_code, display_size, internal_size, precision, scale, null_ok
                (col, 15, None, None, None, None, True)
                for col in self._columns
                ]
        else:
            types = self._get_column_types()
            result =  [
                # name, type_code, display_size, internal_size, precision, scale, null_ok
                (col, self._get_type_code( types[col] ), None, None, None, None, True )
                for col in self._columns
            ]
        return result

    def callproc(self):
        """Drill does not support transactions, so this does nothing."""        
        raise NotSupportedError("Drill does not support stored procedures")  
    
    def execute(self, operation, parameters=None, async=False):
        """Prepare and execute a database operation (query or command).

        Return values are not defined.
        """

        print("Start cursor.execute")

        self._reset_state()
        self._state = self._STATE_RUNNING

        #Clear out newlines in the query:
        operation = operation.replace( '\n',' ')

        #This bit of hackery is needed for SQLAlchemy and Superset
        #Puts backticks in the correct place
        myplugins = self.get_enabled_storage_plugins().values()
        for plugin in myplugins:
            if plugin in operation:
                pattern = plugin.replace( '.', '\.') + r'\.(\S+)'
                tables = re.findall(pattern, operation, re.IGNORECASE)
                for table in tables:
                    tickPattern = r'`(\S+)`'
                    if not re.search(tickPattern, table ):
                        correctedTable = plugin + ".`" + table + "`"
                        oldTable = plugin + "." + table
                        operation = operation.replace( oldTable, correctedTable)

        operation = re.sub(r'SELECT\s+FROM', r'SELECT \* FROM', operation)

        #This bit of hackery is needed for SQLAlchemy and Superset
        #Superset for some reason generates queries where there is no space between the last field and the 'FROM' clause
        badFromPattern = re.search(r'\SFROM\s', operation, flags=re.I )
        if badFromPattern:
            operation = re.sub( r'FROM', ' FROM', operation, re.IGNORECASE)


        operation = operation.replace( "SELECT FROM", "SELECT * FROM")
        operation = operation.replace( '"', '`')
        self._operation = operation

  #      print(self._connectargs)
   #     drill = PyDrill(self._connectargs)
        self._data = self._myconn.query(operation)
        self._actual_cols = self._extract_fields( self._operation )
        self._columns = self._data.columns
        print("Finish cursor execute")

        
    def fetchone(self):
        """Fetch the next row of a query result set, returning a single sequence, or ``None`` when
        no more data is available.
        """
        #if self._state == self._STATE_NONE:
         #   raise exc.ProgrammingError("No query yet")

        # Sleep until we're done or we have some data to return
        #self._fetch_while(lambda: not self._data and self._state != self._STATE_FINISHED)
        if not self._data:
            return None
        elif self._rownumber >= len( self._data.rows ):
            return None
        else:
            result = self._data.rows[self._rownumber]


            correctedData = OrderedDict()
            #Insure correct field order
            for column in self._data.columns:
                correctedData[column] = result[column]

            #result = tuple( result.values() )
            result = tuple( correctedData.values() )
            result = tuple( correctedData.values() )
            self._rownumber += 1
            return result




    def _extract_fields(self, query):
        result = []

        #Case for Show X queryies:
        if query.lower() == "show databases":
            result.append("SCHEMA_NAME")
            return result

        fieldPattern = r'SELECT (.+)\s?FROM'
        aliasPattern = ' (?:AS|as) \S+'
        fieldMatchObj = re.search(fieldPattern, query, re.I)

        if fieldMatchObj:
            fields = fieldMatchObj.group(1)
            if fields.strip() == "*":
                return self._data.columns
            else:
                openParenCount = 0
                fieldName = []

                for char in fields:
                    if char == "," and openParenCount == 0:
                        fieldName = ''.join(fieldName)
                        # Remove alias
                        fieldName = re.sub(aliasPattern, "", fieldName)
                        fieldName = re.sub(r'^\s+', "", fieldName)
                        fieldName = fieldName.strip()
                        fieldName = fieldName.replace('`', '')
                        result.append(fieldName.rstrip())
                        fieldName = []

                    elif char == "(":
                        fieldName.append(char)
                        openParenCount += 1

                    elif char == ")":
                        fieldName.append(char)
                        openParenCount -= 1
                    else:
                        fieldName.append(char)

                fieldName = ''.join(fieldName)
                fieldName = re.sub(aliasPattern, "", fieldName)
                fieldName = re.sub(r'^\s+', "", fieldName)
                fieldName = fieldName.strip()
                fieldName = fieldName.replace('`','')
                result.append(fieldName)

        return result

    def _get_column_types(self):
        columns = self._columns
        types = {}
        query = self._operation

        formattedQuery = sqlparse.format(query, reindent=True, keyword_case='upper')
        formattedQuery = formattedQuery.split('\n')

        inSelect = False
        inSubquery = False
        inFromClause = False
        starQuery = False
        fields = []

        fieldRegex = r'\s{7}\S'
        fieldSubquery = r'\s'
        subqueryFieldPattern = r'\s{2,3}\S'
        subqueryField = ""

        #Case for select * query
        starQueryPattern = r'SELECT\s+\*\s+'
        if re.match(starQueryPattern, query, re.IGNORECASE):
            fields = self._actual_cols
            starQuery = True

        fromClause = ""
        functionPattern = r'\s+(\S+)\('

        fieldCount = 0
        for line in formattedQuery:
            functionMatchObject = re.match(functionPattern, line)
            if line.startswith( 'SELECT') and starQuery == False:
                inSelect = True
                line = line.replace( 'SELECT', '')

                line = line.strip()
                #remove trailing comma
                if len(line) > 0:
                    if line[-1:] == ",":
                        line = line[:-1]
                #Check to see if the field is a function
                functionRegex = re.match(r'([A-Z0-9_]+)\s?\(', line)
                if functionRegex:
                    functionCandidate = functionRegex.group(1)
                    functionCandidate = functionCandidate.upper()
                    if functionCandidate in self._BIG_INT_FUNCTIONS:
                        types[columns[fieldCount]] = "bigint"

                    elif functionCandidate in self._INT_FUNCTIONS:
                        types[columns[fieldCount]] = "integer"

                    elif functionCandidate in self._FLOAT_FUNCTIONS:
                        types[columns[fieldCount]] = "float"
                    else:
                        # TODO Special Case for CAST() and TO_ functions
                        types[columns[fieldCount]] = "varchar"
                else:
                    fields.append(line)

            elif starQuery == True and line.startswith( 'SELECT'):
                inSelect = True

            # If the line is a function, assign the correct return type
            elif inSelect and inFromClause == False and functionMatchObject and starQuery == False:
                functionCandidate = functionMatchObject.group(1)
                functionCandidate = functionCandidate.upper()
                if functionCandidate in self._BIG_INT_FUNCTIONS:
                    types[columns[fieldCount]] = "bigint"

                elif functionCandidate in self._INT_FUNCTIONS:
                    types[columns[fieldCount]] = "integer"

                elif functionCandidate in self._FLOAT_FUNCTIONS:
                    types[columns[fieldCount]] = "float"
                else:
                    #TODO Special Case for CAST() and TO_ functions
                    types[columns[fieldCount]] = "varchar"

            # Case for a regular field
            elif inSelect == True and re.match(fieldRegex, line) and starQuery == False:
                line = line.strip()
                # remove trailing comma from field name
                if len(line) > 0:
                    if line[-1:] == ",":
                        line = line[:-1]
                fields.append(line)

            elif inSelect == True and line.startswith('FROM'):
                inSelect = False
                inFromClause = True
                if inSubquery and starQuery == False:
                    fields.append( subqueryField )
                    inSubquery = False
                else:
                    fromClause = fromClause + " " + line.strip()

            elif inFromClause == True and (line.startswith( 'WHERE') or line.startswith('GROUP') or line.startswith('ORDER') or line.startswith('HAVING')):
                inFromClause = False
                inSelect = False
                break

            elif re.match(subqueryFieldPattern, line) and inSubquery == False and inFromClause == False and starQuery == False:
                inSubquery = True
                subqueryField = line.strip()

            elif inSubquery == True and starQuery == False:
                subqueryField = subqueryField + " " + line.strip()
                if line.endswith( ','):
                    inSubquery = False
                    fields.append(subqueryField)
                    subqueryField = ""

            elif inSubquery == True and line == False and starQuery == False:
                inSubquery = False
                fields.append(subqueryField)
                subqueryField = ""

            elif inFromClause == True:
                fromClause = fromClause + " " + line.strip()

            fieldCount += 1

        #If the query was all functions, return types are known and return the array
        if len( fields) == 0:
            return types

        typeQuery = "SELECT "
        fieldCount = 0
        aliasPattern = r'AS\s`?[a-zA-Z_][a-zA-Z0-9-_$` ]*$'
        for field in fields:
            if re.search(aliasPattern, field):
                field = re.sub(aliasPattern, '', field)

            if fieldCount > 0:
                typeQuery += ","

            if starQuery == True:
                if not columns[fieldCount].startswith( '`'):
                    typeQuery = typeQuery + "`" + columns[fieldCount] + "`" + ", typeof( `" + columns[fieldCount] + "`) AS `" + \
                                columns[fieldCount] + "_type`"
                else:
                    typeQuery = typeQuery + columns[fieldCount] + ", typeof( " + columns[fieldCount] + ") AS " + \
                            columns[fieldCount] + "_type"
            else:
                if not field.startswith( '`'):
                    typeQuery = typeQuery + " `" + field + "` AS `" + columns[fieldCount] + "`, typeof( `" + field + "` ) AS `" + columns[fieldCount] + "_type`"
                elif field.startswith( '`') and not columns[fieldCount].startswith( '`'):
                    typeQuery = typeQuery + " " + field + " AS `" + columns[
                        fieldCount] + "`, typeof( " + field + " ) AS `" + columns[fieldCount] + "_type`"
                else:
                    typeQuery = typeQuery + " " + field + " AS " + columns[
                        fieldCount] + ", typeof( " + field + " ) AS " + columns[fieldCount] + "_type"

            fieldCount += 1

        #Remove Limit clause in From clause
        limitPattern = r'LIMIT \d+$'
        if re.search(limitPattern, fromClause):
            fromClause = re.sub( limitPattern, '', fromClause)

        typeQuery += fromClause

        typeQuery += " LIMIT 1"
        typeQuery = sqlparse.format(typeQuery, reindent=True, keyword_case='upper')

        #drill = PyDrill(host=self._host, port=self._port)
        fieldQueryResult = self._myconn.query(typeQuery).to_dataframe()
        tempTypes = fieldQueryResult.T.to_dict()[0]

        for column in columns:
            if column not in types.keys():
                types[ column ] = tempTypes[ column + "_type"]


        return types

    def get_enabled_storage_plugins(self):
        """
        :param self:
        :return: List of enabled storage plugins
        """
        #drill = PyDrill(host=self._host, port=self._port)
        plugins = self._myconn.query("SHOW DATABASES").to_dataframe().to_dict()
        return plugins['SCHEMA_NAME']

    def _get_type_code(self, type):
        if type == "BIGINT":
            return "bigint"
        elif type == "BINARY":
            return "binary"
        elif type =="BOOLEAN":
            return "boolean"
        elif type == "DATE":
            return "date"
        elif type == "DECIMAL" or type == "DEC" or type == "NUMERIC":
            return "decimal"
        elif type == "FLOAT" or type == "FLOAT4":
            return "float"
        elif type == "DOUBLE" or type == "DOUBLE PRECISION" or type == "FLOAT8":
            return "double"
        elif type == "INTEGER" or type == "INT":
            return "integer"
        elif type == "INTERVAL":
            return "interval"
        elif type == "SMALLINT":
            return "smallint"
        elif type == "TIME":
            return "time"
        elif type == "TIMESTAMP":
            return "timestamp"
        elif type == "VARCHAR":
            return "varchar"
        else:
            return "varchar"


class DictCursorMixin(object):
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    def _do_get_result(self):
        super(DictCursorMixin, self)._do_get_result()
        fields = []
        if self.description:
            for f in self._result.fields:
                name = f.name
                if name in fields:
                    name = f.table_name + '.' + name
                fields.append(name)
            self._fields = fields

        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_row(self, row):
        if row is None:
            return None
        return self.dict_type(zip(self._fields, row))


class DictCursor(DictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary"""
