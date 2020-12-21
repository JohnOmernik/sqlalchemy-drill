# -*- coding: utf-8 -*-
from json import dumps
import pandas as pd
from requests import Session
import re
import logging
from . import api_globals
from .api_exceptions import AuthError, DatabaseError, ProgrammingError, CursorClosedException, \
    ConnectionClosedException

apilevel = '2.0'
threadsafety = 3
paramstyle = 'qmark'
default_storage_plugin = ""

DRILL_PANDAS_TYPE_MAP = {
        'BIGINT': 'Int64',
        'BINARY': 'object',
        'BIT':  'boolean' if pd.__version__ >= '1' else 'bool',
        'DATE': 'datetime64[ns]',
        'FLOAT4': 'float32',
        'FLOAT8': 'float64',
        'INT': 'Int32',
        'INTERVALDAY': 'string' if pd.__version__ >= '1' else 'object',
        'INTERVALYEAR': 'string' if pd.__version__ >= '1' else 'object',
        'SMALLINT': 'Int32',
        'TIME': 'string' if pd.__version__ >= '1' else 'object',
        'TIMESTAMP': 'datetime64[ns]',
        'VARDECIMAL': 'object',
        'VARCHAR' : 'string' if pd.__version__ >= '1' else 'object'
        }

logging.basicConfig(level=logging.WARN)
logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s')

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
        self._resultSetMetadata = None
        self._resultSetStatus = None
        self.rowcount = -1

    # Decorator for methods which require connection
    def connected(func):
        def func_wrapper(self, *args, **kwargs):
            if self._connected is False:
                logging.error("Error in Cursor.func_wrapper")
                raise CursorClosedException("Cursor object is closed")
            elif self.connection._connected is False:
                logging.error("Error in Cursor.func_wrapper")
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
            logging.error("Error in Cursor.substitute_in_query" + str(ex))
        return query

    @staticmethod
    def submit_query(query, host, port, proto, session):
        local_payload = api_globals._PAYLOAD.copy()
        local_payload["query"] = query
        return session.post(
            proto + host + ":" + str(port) + "/query.json",
            data=dumps(local_payload),
            headers=api_globals._HEADER,
            timeout=None
        )

    @staticmethod
    def parse_column_types(metadata):
        names = []
        types = []
        for row in metadata:
            names.append(row['column'])
            types.append(row['type'].lower())

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

        matchObj = re.match(r'^SHOW FILES FROM\s(.+)', operation, re.IGNORECASE)
        if matchObj:
            self.default_storage_plugin = matchObj.group(1)

        if result.status_code != 200:
            logging.error("Error in Cursor.execute")
            raise ProgrammingError(result.json().get("errorMessage", "ERROR"), result.status_code)
        else:
            result_json = result.json()
            self.cols = result_json["columns"]
            self.columns = self.cols
            self.metadata = result_json["metadata"]

            # Get column metadata
            column_metadata = []
            for i in range(0, len(self.cols)):
                col = {
                    "column": self.cols[i],
                    "type": self.metadata[i]
                }
                column_metadata.append(col)

            self._resultSetMetadata = column_metadata

            df = pd.DataFrame(result_json["rows"], columns=result_json["columns"])

            # The columns in df all have a dtype of object because Drill's
            # HTTP API always quotes the values in the JSON it returns, thereby
            # providing DataFrame(...) with a dict of strings.  We now use
            # the metadata returned by Drill to correct this
            for i in range(len(self.columns)):
                col_name = self.columns[i]
                # strip any precision information that might be in the metdata e.g. VARCHAR(10)
                col_drill_type = re.sub(r'\(.*\)', '', self.metadata[i])

                if col_drill_type not in DRILL_PANDAS_TYPE_MAP:
                    logging.warning("Warning: could not map Drill column {} of type {} to a Pandas dtype".format(self.cols[i], self.metadata[i]))
                else:
                    col_dtype = DRILL_PANDAS_TYPE_MAP[col_drill_type]
                    logging.debug('Mapping column {} of Drill type {} to dtype {}'.format(col_name, col_drill_type, col_dtype))

                    # Null values cause problems, so first verify if there are null values in the column
                    if df[col_name].isnull().values.any():
                        can_cast = False
                    elif str(df[col_name].iloc[0]).startswith("[") and str(df[col_name].iloc[0]).endswith("]"):
                        can_cast = False
                    else:
                        can_cast = True

                        if col_drill_type == 'BIT':
                            if pd.__version__ < '1' and df[col_name].isna().any():
                                logger.warn('Null boolean values will be coerced to False!  Upgrade to Pandas >= 1.0 for nullable booleans.')
                            df[col_name] = df[col_name].apply(lambda b: b == 'true' if b else None)
                        # Commenting this out for the time being. Pandas does not seem to support time data types (times with no dates) and hence
                        # this functionality breaks Superset. 
                        #elif col_drill_type == 'TIME': # col_name in ['TIME', 'INTERVAL']: # parsing of ISO-8601 intervals appears broken as of Pandas 1.0.3
                            #logging.warning("Time Column: {} {}".format(col_name, df[col_name].iloc[0]))
                            #df[col_name] = pd.to_timedelta(df[col_name])
                            #df[col_name] = pd.to_datetime()
                        elif col_drill_type in ['FLOAT4', 'FLOAT8']:
                            # coerce errors when parsing floats to handle 'NaN' ('Infinity' is fine)
                            df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                        elif col_drill_type in ['BIGINT', 'INT', 'SMALLINT']:
                            df[col_name] = pd.to_numeric(df[col_name])
                            if df[col_name].isnull().values.any():
                                logging.warning('Column {} of Drill type {} contains nulls so cannot be converted to an integer dtype in Pandas < 1.0.0'.format(col_name, col_drill_type))
                                can_cast = False

                    if can_cast:
                        df[col_name] = df[col_name].astype(col_dtype)

            self._resultSet = df

            self.rowcount = len(self._resultSet)
            self._resultSetStatus = iter(range(len(self._resultSet)))
            column_names, column_types = self.parse_column_types(self._resultSetMetadata)

            try:
                self.description = tuple(
                    zip(
                        column_names,
                        self.metadata,
                        [None for i in range(len(self._resultSet.dtypes.index))],
                        [None for i in range(len(self._resultSet.dtypes.index))],
                        [None for i in range(len(self._resultSet.dtypes.index))],
                        [None for i in range(len(self._resultSet.dtypes.index))],
                        [True for i in range(len(self._resultSet.dtypes.index))]
                    )
                )
                return self
            except Exception as ex:
                logging.error(("Error in Cursor.execute", str(ex)))

    @connected
    def fetchone(self):
        try:
            # Added Tuple
            return self._resultSet.iloc[next(self._resultSetStatus)]
        except StopIteration:
            logging.debug("Caught StopIteration in fetchone")
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
            logging.debug("Caught StopIteration in fetchmany")
            return None

    @connected
    def fetchall(self):
        # We can't just return a dataframe to sqlalchemy, it has to be a list of tuples...
        try:
            remaining = self._resultSet[next(self._resultSetStatus):]
            self._resultSetStatus = iter(tuple())
            return [tuple(x) for x in remaining.to_records(index=False)]

        except StopIteration:
            logging.debug("Caught StopIteration in fetchall")
            logging.debug((StopIteration.value, StopIteration.with_traceback()))
            return None

    @connected
    def get_query_metadata(self):
        return self._resultSetMetadata

    def get_default_plugin(self):
        return self.default_storage_plugin

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
                logging.error("ConnectionClosedException in func_wrapper")
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
            logging.error("AlreadyClosedException")
        else:
            logging.warning("Here goes some sort of commit")

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
        logging.error("Error in connect")
        raise DatabaseError(str(response.json()["errorMessage"]), response.status_code)
    else:
        raw_data = response.text
        if raw_data.find("Invalid username/password credentials") >= 0:
            logging.error("Error in connect: ", response.text)
            raise AuthError(str(raw_data), response.status_code)

        if db is not None:
            local_payload = api_globals._PAYLOAD.copy()
            local_url = "/query.json"
            # local_payload["query"] = "USE {}".format(db)
            local_payload["query"] = "SELECT 'test' FROM (VALUES(1))"

            response = session.post(
                "{proto}{host}:{port}{url}".format(proto=proto, host=host, port=str(port), url=local_url),
                data=dumps(local_payload),
                headers=api_globals._HEADER
            )

            if response.status_code != 200:
                logging.error("Error in connect")
                logging.error("Response code:", response.status_code)
                raise DatabaseError(str(response.json()["errorMessage"]), response.status_code)

        return Connection(host, db, port, proto, session)
