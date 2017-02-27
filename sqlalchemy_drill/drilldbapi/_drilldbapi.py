# coding: utf-8

from json import dumps
from numpy import nan
from pandas import Series, DataFrame, to_datetime
from requests import Session

# Globals
apilevel = '2.0'
threadsafety = 3
paramstyle = 'qmark'
_HEADER = {"Content-Type": "application/json"}
_PAYLOAD = {"queryType":"SQL", "query": None}
_LOGIN = {"j_username": None, "j_password": None}



# Exceptions
class Warning(Exception):
    pass

class Error(Exception):
    pass

class AuthError(Error):
    def __init__(self, message, httperror):
        self.message = message
        self.httperror = httperror
    def __str__(self):
        return repr(self.message + " Authentication Error: Invalid User/Pass: %s" % self.httperror)


class DatabaseError(Error):
    def __init__(self, message, httperror):
        self.message = message
        self.httperror = httperror
    def __str__(self):
        return repr(self.message + " HTTP ERROR: %s" % self.httperror)

class ProgrammingError(DatabaseError):
    def __init__(self, message, httperror):
        self.message = message
        self.httperror = httperror
    def __str__(self):
        return repr(self.message + " HTTP ERROR: %s" % self.httperror)

class CursorClosedException(Error):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return repr(self.message)

class ConnectionClosedException(Error):
    def __init__(self, message):
        self.message = message
    def __str__(self):
        return repr(self.message)
# Object types
class STRING(type):
    pass

class NUMBER(type):
    pass

class DATETIME(type):
    pass

# Helper functions
def substitute_in_query(string_query, parameters):
    query = string_query
    for param in parameters:
        if type(param) == str:
            query = query.replace("?", "'" + param + "'", 1)
        else:
            query = query.replace("?", str(param), 1)
    return query

def submit_query(query, host, port, proto, session):
    local_payload = _PAYLOAD.copy()
    local_payload["query"] = query
    return session.post(proto
                        + host
                        + ":"
                        + str(port)
                        + "/query.json",
                        data = dumps(local_payload),
                        headers = _HEADER)

def parse_column_types(df):
    names = []
    types = []
    for column in df:
        try:
            df[column] = df[column].astype(int)
            types.append("bigint")
            names.append(column)
        except ValueError:
            try:
                df[column] = df[column].astype(float)
                types.append("decimal")
                names.append(column)
            except ValueError:
                try:
                    df[column] = to_datetime(df[column])
                    types.append("timestamp")
                    names.append(column)
                except ValueError:
                    types.append("varchar")
                    names.append(column)
    return (names, types)
                

# Python DB API 2.0 classes
class Cursor(object):
    def __init__(self, host, db, port, proto, session, conn):
        self.arraysize = 1
        self.db = db
        self.description = None
        self.host = host
        self.port = port
        self.proto = proto
        self._session = session
        self._connected = True
        self.connection = conn
        self._resultSet = None
        self._resultSetStatus = None

    # Decorator for methods which require connection
    def connected(func):
        def func_wrapper(self, *args, **kwargs):
            if self._connected == False:
                raise CursorClosedException("Cursor object is closed")
            elif self.connection._connected == False:
                raise ConnectionClosedException("Connection object is closed")
            else:
                return func(self, *args, **kwargs)
        return func_wrapper

    @connected
    def close(self):
        self._connected = False

    @connected
    def execute(self, operation, parameters=()):
        result = submit_query(substitute_in_query(operation, parameters),
                              self.host,
                              self.port,
                              self.proto,
                              self._session)
            
        if result.status_code != 200:
            raise ProgrammingError(result.json()["errorMessage"], result.status_code)
        else:
            self._resultSet = (DataFrame(result.json()["rows"],
                                        columns = result.json()["columns"])
                               .fillna(value=nan))
            self._resultSetStatus = iter(range(len(self._resultSet)))
            column_names, column_types = parse_column_types(self._resultSet)
            self.description = tuple(
                zip(column_names,
                    column_types,
                    [None for i in range(len(self._resultSet.dtypes.index))],
                    [None for i in range(len(self._resultSet.dtypes.index))],
                    [None for i in range(len(self._resultSet.dtypes.index))],
                    [None for i in range(len(self._resultSet.dtypes.index))],
                    [True for i in range(len(self._resultSet.dtypes.index))]
                )
            )
            return self


    @connected
    def getdesc(self):
        return self.description

    @connected
    def fetchone(self):
        print("####### IN DBAPI fetchone")
        try:
            return self._resultSet.ix[next(self._resultSetStatus)] # Added Tuple
        except StopIteration:
            return None  # We need to put None rather than Series([]) because SQLAlchemy processes that a row with no columns which it doesn't like
#            return Series([])

    @connected
    def fetchmany(self, size=None):
        print("######## In DBAPI fetchmany")
        fetch_size = 1
        if size == None:
            fetch_size = self.arraysize
        else:
            fetch_size = size

        print("fetch_size" + str(fetch_size))
        results = []
        try:
            index = next(self._resultSetStatus)
            try:
                for element in range(fetch_size - 1):
                    next(self._resultSetStatus)
            except StopIteration:
                pass
            myresults = self._resultSet[index : index + fetch_size]

            return [tuple(x) for x in myresults.to_records(index=False)]
            #return self._resultSet[index : index + fetch_size]
        except StopIteration:
            return None
            #return Series([])

    @connected
    def fetchall(self):
        print("######### IN DBAPI fetchall")
        # We can't just return a dataframe to sqlalchemy, it has to be a list of tuples... 
        try:
            remaining = self._resultSet[next(self._resultSetStatus):]
            self._resultSetStatus = iter(tuple())
            all = [tuple(x) for x in remaining.to_records(index=False)]
    
            return all
        except StopIteration:
            return None
           # return Series([])

    def __iter__(self):
        return self._resultSet.iterrows()

class Connection(object):
    def __init__(self, host, db, port, proto, session):
        self.host = host
        self.db = db
        self.proto = proto
        self.port = port
        self._session = session
        self._connected = True

    # Decorator for methods which require connection
    def connected(func):
        def func_wrapper(self, *args, **kwargs):
            if self._connected == False:
                raise ConnectionClosedException("Connection object is closed")
            else:
                return func(self, *args, **kwargs)
        return func_wrapper

    @connected
    def close(self):
        self._connected = False

    @connected
    def commit(self):
        if self._connected == False:
            print("AlreadyClosedException")
        else:
            print("Here goes some sort of commit")

    @connected
    def cursor(self):
        return Cursor(self.host, self.db, self.port, self.proto, self._session, self)

def connect(host, port=8047, db=None, use_ssl=False, drilluser=None, drillpass=None, verify_ssl=False, ca_certs=None):
    session = Session()

    if verify_ssl == False:
        session.verify = False
    else:
        if ca_certs != None:
            session.verify = ca_certs
        else:
            session.verify = True
    
    if drilluser == None:
        local_payload = _PAYLOAD.copy()
        local_url = "/query.json"
        local_payload["query"] = "show schemas"
    else:
        local_payload = _LOGIN.copy()
        local_payload["j_username"] = drilluser
        local_payload["j_password"] = drillpass
        local_url = "/j_security_check"
    if use_ssl == False:
        proto = "http://"
    else:
        proto = "https://"

    if drilluser == None:
        response = session.post(proto + host + ":" + str(port) + local_url,
                             data = dumps(local_payload),
                             headers = _HEADER)
    else:
        response = session.post(proto + host + ":" + str(port) + local_url,
                             data = local_payload)


    if response.status_code != 200:
        raise DatabaseError(str(response.json()["errorMessage"]),
                             response.status_code)
    else:
        raw_data = response.text
        if raw_data.find("Invalid username/password credentials") >= 0:
            raise AuthError(str(raw_data), response.status_code)
        if db != None:
            local_payload = _PAYLOAD.copy()
            local_url = "/query.json"
            local_payload["query"] = "USE {}".format(db)
            response = session.post(proto + host + ":" + str(port) + local_url,
                             data = dumps(local_payload),
                             headers = _HEADER)
            if response.status_code != 200:
                raise DatabaseError(str(response.json()["errorMessage"]),
                             response.status_code)


        return Connection(host, db, port, proto, session)


