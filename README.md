# Apache Drill dialect for SQLAlchemy.

---

The primary purpose of this is to have a working dialect for Apache Drill that can be used with Apache Superset.

https://superset.incubator.apache.org

Obviously, a working, robust dialect for Drill serves other purposes as well, but most of the iterative planning for this REPO will be based on working with Superset. Other changes will gladly be incorporated, as long as it doesn't hurt Superset integration.

## Installation

Installing the dialect is straightforward. Simply:

```
pip install sqlalchemy-drill
```

Alternatively, you can download the latest release from github and install from here:

```python
python3 -m pip install git+https://github.com/JohnOmernik/sqlalchemy-drill.git
```

## Usage with REST

Drill's REST API can execute queries with results streamed to JSON returned over chunked HTTP for Drill >= 1.19, otherwise with results buffered and then returned in a conventional HTTP response.  A SQLAlchemy URL to connect to Drill over REST looks like the following.

```
drill+sadrill://<username>:<password>@<host>:<port>/<storage_plugin>?use_ssl=True
```

To connect to Drill running on a local machine running in embedded mode you can use the following connection string.

```
drill+sadrill://localhost:8047/dfs?use_ssl=False
```

### Supported URL query parameters

| Parameter                 | Type    | Description                                                    |
| ------------------------- | ------- | -------------------------------------------------------------- |
| use_ssl                   | boolean | Whether to connect to Drill using HTTPS                        |
| verify_ssl                | boolean | Whether to verify the server's TLS certificate                 |
| impersonation_target\[1\] | string  | Username of a Drill user to be impersonated by this connection |

[1] Requires a build of Drill that incorporates the fix for DRILL-8168.

### Trailing metadata

Query result metadata returned by the Drill REST API is stored in the `result_md` field of the DB-API Cursor object.  Note that any trailing metadata, i.e. metadata which comes after result row data, will only be populated after you have iterated through all of the returned rows.  If you need this trailing metadata you can make the cursor object reachable after it has been completely iterated by obtaining a reference to it beforehand, as follows.

```python
r = engine.execute('select current_timestamp')
r.cursor.result_md  # access metadata, but only leading metadata
cur = r.cursor      # obtain a reference for use later
r.fetchall()        # iterate through all result data
cur.result_md       # access metadata, including trailing metadata
del cur             # optionally delete the reference when done
```

### Drill < 1.19

In versions of Drill earlier than 1.19, all data values are serialised to JSON strings and column type metadata comes after the data itself.  As a result, for these versions of Drill, the drill+sadrill dialect returns every data value as a string.  To convert non-string data to its native type you need to typecast it yourself.

### Drill >= 1.19

In Drill 1.19 the REST API began making use of numeric types in JSON for numbers and times, the latter via a UNIX time representation while column type metadata was moved ahead of the result data.  Because of this, the drill+sadrill dialect is able to return appropriate types for numbers and times when used with Drill >= 1.19.

## Usage with JDBC

Connecting to Drill via JDBC is a little more complicated than a local installation and complete instructions can be found on the Drill documentation here: https://drill.apache.org/docs/using-the-jdbc-driver/.

In order to configure SQLAlchemy to work with Drill via JDBC you must:

- Download the latest JDBC Driver available here: http://apache.osuosl.org/drill/
- Copy this driver to your classpath or other known path
- Set an environment variable called `DRILL_JDBC_DRIVER_PATH` to the full path of your driver location
- Set an environment variable called `DRILL_JDBC_JAR_NAME` to the name of the `.jar` file for the Drill driver.

Additionally, you will need to install `JayDeBeApi` as well as jPype version 0.6.3.
These modules are listed as optional dependencies and will not be installed by the default installer.

If the JDBC driver is not available, the dialect will throw errors when trying
to connect. In addition, sqlalchemy-drill will not launch a JVM for you so you
need to do this yourself with a call to JPype like the following. See the file
test-jdbc.py in this repo for a working example.

```python
jpype.startJVM("-ea", classpath="lib/*")
```

```
drill+jdbc://<username>:<passsword>@<host>:<port>
```

For a simple installation, this might look like:

```
drill+jdbc://admin:password@localhost:31010
```

## Usage with ODBC

In order to configure SQLAlchemy to work with Drill via ODBC you must:

- Install latest Drill ODBC Driver: https://drill.apache.org/docs/installing-the-driver-on-linux/
- Ensure that you have ODBC support in your system (`unixODBC` package for RedHat-based systems).
- Install `pyodbc` Python package.
  This module is listed as an optional dependency and will not be installed by the default installer.

To connect to Drill with SQLAlchemy use the following connection string:

```
drill+odbc:///?<ODBC connection parameters>
```

Connection properties are available in the official documentation: https://drill.apache.org/docs/odbc-configuration-reference/

For a simple installation, this might look like:

```
drill+odbc:///?Driver=/opt/mapr/drill/lib/64/libdrillodbc_sb64.so&ConnectionType=Direct&HOST=localhost&PORT=31010&AuthenticationType=Plain&UID=admin&PWD=password
```

or for the case when you have DSN configured in `odbc.ini`:

```
drill+odbc:///?DSN=drill_dsn_name
```

**Note:** it's better to avoid using connection string with `hostname:port` or `username`/`password`, like 'drill+odbc://admin:password@localhost:31010/' but use only ODBC properties instead to avoid any misinterpretation between these parameters.

## Usage with Superset

For a complete tutorial on how to use Superset with Drill, read the tutorial on @cgivre's blog available here: http://thedataist.com/visualize-anything-with-superset-and-drill/.

## Current Status/Development Approach

Currently we can connect to drill, and issue queries for most visualizations and get results. We also enumerate table columns for some times of tables. Here are things that are working as some larger issues to work out. (Individual issues are tracked under issues)

- Connection to Drill via the databases tab in Superset succeeds
- You can do basic queries for most types of viz/tables
- There may be issues with advanced queries/joins. As you learn about new ones, please track in issues

### Many thanks

to drillpy and pydrill for code used in creating the original `drilldbapi.py` code for connecting!

### Docker

It is recommended to extend [the official Docker image](https://hub.docker.com/r/apache/superset) to include this Apache Drill driver:

```dockerfile
FROM apache/superset
# Switching to root to install the required packages
USER root
RUN pip install git+https://github.com/JohnOmernik/sqlalchemy-drill.git
# Switching back to using the `superset` user
USER superset
```
