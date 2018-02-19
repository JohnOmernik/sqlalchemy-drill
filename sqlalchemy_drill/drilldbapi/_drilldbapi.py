# -*- coding: utf-8 -*-
from json import dumps
from numpy import nan
from pandas import DataFrame
from requests import Session
from pandas import to_datetime

from . import api_globals
from .api_exceptions import Error, Warning, AuthError, DatabaseError, ProgrammingError, CursorClosedException, ConnectionClosedException

apilevel = '2.0'
threadsafety = 3
paramstyle = 'qmark'


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
        self.rowcount = -1

    # Decorator for methods which require connection
    def connected(func):
        def func_wrapper(self, *args, **kwargs):
            if self._connected is False:
                print("************************************")
                print("Error in Cursor.func_wrapper")
                print("************************************")
                raise CursorClosedException("Cursor object is closed")
            elif self.connection._connected is False:
                print("************************************")
                print("Error in Cursor.func_wrapper")
                print("************************************")
                raise ConnectionClosedException("Connection object is closed")
            else:
                return func(self, *args, **kwargs)

        return func_wrapper

    @staticmethod
    def substitute_in_query(string_query, parameters):
        query = string_query
        try:
            for param in parameters:
                if type(param) == str:
                    query = query.replace("?", "'{param}'".format(param=param), 1)
                else:
                    query = query.replace("?", str(param), 1)
        except Exception as ex:
            print("************************************")
            print("Error in Cursor.substitute_in_query", str(ex))
            print("************************************")
        return query

    @staticmethod
    def submit_query(query, host, port, proto, session):
        local_payload = api_globals._PAYLOAD.copy()
        local_payload["query"] = query
        return session.post(
            proto + host + ":" + str(port) + "/query.json",
            data=dumps(local_payload),
            headers=api_globals._HEADER
        )

    @staticmethod
    def parse_column_types(df):
        names = []
        types = []
        try:
            for column in df:
                names.append(column)
                try:
                    df[column] = df[column].astype(int)
                    types.append("bigint")
                except ValueError:
                    try:
                        df[column] = df[column].astype(float)
                        types.append("decimal")
                    except ValueError:
                        try:
                            df[column] = to_datetime(df[column])
                            types.append("timestamp")
                        except ValueError:
                            types.append("varchar")
        except Exception as ex:
            print("************************************")
            print("Error in Cursor.parse_column_types", str(ex))
            print("************************************")
        return names, types

    @connected
    def getdesc(self):
        return self.description

    @connected
    def close(self):
        self._connected = False

    @connected
    def execute(self, operation, parameters=()):
        result = self.submit_query(
            self.substitute_in_query(operation, parameters),
            self.host,
            self.port,
            self.proto,
            self._session
        )

        if result.status_code != 200:
            print("************************************")
            print("Error in Cursor.execute")
            print("************************************")
            raise ProgrammingError(result.json().get("errorMessage", "ERROR"), result.status_code)
        else:
                self._resultSet = (
                    DataFrame(
                        result.json()["rows"],
                        columns=result.json()["columns"]
                    ).fillna(value=nan)
                )
                self.rowcount = len(self._resultSet)
                self._resultSetStatus = iter(range(len(self._resultSet)))
                column_names, column_types = self.parse_column_types(self._resultSet)

                try:
                    self.description = tuple(
                        zip(
                            column_names,
                            column_types,
                            [None for i in range(len(self._resultSet.dtypes.index))],
                            [None for i in range(len(self._resultSet.dtypes.index))],
                            [None for i in range(len(self._resultSet.dtypes.index))],
                            [None for i in range(len(self._resultSet.dtypes.index))],
                            [True for i in range(len(self._resultSet.dtypes.index))]
                        )
                    )
                    return self
                except Exception as ex:
                    print("************************************")
                    print("Error in Cursor.execute", str(ex))
                    print("************************************")

    @connected
    def fetchone(self):
        try:
            # Added Tuple
            return self._resultSet.ix[next(self._resultSetStatus)]
        except StopIteration:
            print("************************************")
            print("Catched StopIteration in fetchone")
            print("************************************")
            # We need to put None rather than Series([]) because
            # SQLAlchemy processes that a row with no columns which it doesn't like
            return None

    @connected
    def fetchmany(self, size=None):

        if size is None:
            fetch_size = self.arraysize
        else:
            fetch_size = size

        try:
            index = next(self._resultSetStatus)
            try:
                for element in range(fetch_size - 1):
                    next(self._resultSetStatus)
            except StopIteration:
                pass

            myresults = self._resultSet[index: index + fetch_size]
            return [tuple(x) for x in myresults.to_records(index=False)]
        except StopIteration:
            print("************************************")
            print("Catched StopIteration in fetchmany")
            print("************************************")
            return None

    @connected
    def fetchall(self):
        # We can't just return a dataframe to sqlalchemy, it has to be a list of tuples...
        try:
            remaining = self._resultSet[next(self._resultSetStatus):]
            self._resultSetStatus = iter(tuple())
            return [tuple(x) for x in remaining.to_records(index=False)]

        except StopIteration:
            print("************************************")
            print("Catched StopIteration in fetchall")
            print("************************************")
            return None

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
            if self._connected is False:
                print("************************************")
                print("ConnectionClosedException in func_wrapper")
                print("************************************")
                raise ConnectionClosedException("Connection object is closed")
            else:
                return func(self, *args, **kwargs)
        return func_wrapper

    def is_connected(self):
        try:
            if self._connected is True:
                if self._session:
                    return True
                else:
                    self._connected = False
        except Exception:
            print('*************************')
            print("Error in Connection.is_connected")
            print('*************************')
            print(Exception)
        return False

    @connected
    def close_connection(self):
        try:
            self._session.close()
            self.close()
        except Exception:
            print('*************************')
            print("Error in Connection.close_connection")
            print('*************************')
            print(Exception)
            return False
        return True

    @connected
    def close(self):
        self._connected = False

    @connected
    def commit(self):
        if self._connected is False:
            print("AlreadyClosedException")
        else:
            print("Here goes some sort of commit")

    @connected
    def cursor(self):
        return Cursor(self.host, self.db, self.port, self.proto, self._session, self)


def connect(host, port=8047, db=None, use_ssl=False, drilluser=None, drillpass=None, verify_ssl=False, ca_certs=None):
    session = Session()

    if verify_ssl is False:
        session.verify = False
    else:
        if ca_certs is not None:
            session.verify = ca_certs
        else:
            session.verify = True

    if use_ssl in [True, 'True', 'true']:
        proto = "https://"
    else:
        proto = "http://"

    if drilluser is None:
        local_url = "/query.json"
        local_payload = api_globals._PAYLOAD.copy()
        local_payload["query"] = "show schemas"
        response = session.post(
            "{proto}{host}:{port}{url}".format(proto=proto, host=host, port=str(port), url=local_url),
            data=dumps(local_payload),
            headers=api_globals._HEADER
        )
    else:
        local_url = "/j_security_check"
        local_payload = api_globals._LOGIN.copy()
        local_payload["j_username"] = drilluser
        local_payload["j_password"] = drillpass
        response = session.post(
            "{proto}{host}:{port}{url}".format(proto=proto, host=host, port=str(port), url=local_url),
            data=local_payload
        )

    if response.status_code != 200:
        print("************************************")
        print("Error in connect")
        print("************************************")
        raise DatabaseError(str(response.json()["errorMessage"]), response.status_code)
    else:
        raw_data = response.text
        if raw_data.find("Invalid username/password credentials") >= 0:
            print("************************************")
            print("Error in connect")
            print("************************************")
            raise AuthError(str(raw_data), response.status_code)

        if db is not None:
            local_payload = api_globals._PAYLOAD.copy()
            local_url = "/query.json"
            local_payload["query"] = "USE {}".format(db)

            response = session.post(
                "{proto}{host}:{port}{url}".format(proto=proto, host=host, port=str(port), url=local_url),
                data=dumps(local_payload),
                headers=api_globals._HEADER
            )

            if response.status_code != 200:
                print("************************************")
                print("Error in connect")
                print("************************************")
                raise DatabaseError(str(response.json()["errorMessage"]), response.status_code)

        return Connection(host, db, port, proto, session)
