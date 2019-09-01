# Apache Drill dialect for SQLAlchemy.
---
The primary purpose of this is to have a working dialect for Apache Drill that can be used with Apache Superset.

https://superset.incubator.apache.org

Obviously, a working, robust dialect for Drill serves other purposes as well, but most of the iterative planning for this REPO will be based on working with Superset. Other changes will gladly be incorporated, as long as it doesn't hurt Superset integration. 

## Installation 
Installing the dialect is straightforward.  Simply:
1.  Clone or download this repository
2.  Navigate to the directory where you cloned the repo
3.  Run the python `setup.py` to install

Examples are shown below
```
git clone https://github.com/JohnOmernik/sqlalchemy-drill
cd sqlalchemy-drill
python3 setup.py install 

```

## Usage
To use Drill with SQLAlchemy you will need to craft a connection string in the format below:

```
drill+sadrill://<username>:<password>@<host>:<port>/<storage_plugin>?use_ssl=True
```

To connect to Drill running on a local machine running in embedded mode you can use the following connection string.  
```
drill+sadrill://localhost:8047/dfs?use_ssl=False
```

## Usage with JDBC
Connecting to Drill via JDBC is a little more complicated than a local installation and complete instructions can be found on the Drill documentation here: https://drill.apache.org/docs/using-the-jdbc-driver/.

In order to configure SQLAlchemy to work with Drill via JDBC you must:
* Download the latest JDBC Driver available here: http://apache.osuosl.org/drill/
* Copy this driver to your classpath or other known path
* Set an environment variable called `DRILL_JDBC_DRIVER_PATH` to the full path of your driver location
* Set an environment variable called `DRILL_JDBC_JAR_NAME` to the name of the `.jar` file for the Drill driver.

Additionally, you will need to install `JayDeBeApi` as well as jPype version 0.6.3.  
These modules are listed as optional dependencies and will not be installed by the default installer. 

If the JDBC driver is not available, the dialect will throw errors when trying to connect.

```
drill+jdbc://<username>:<passsword>@<host>:<port>
```
For a simple installation, this might look like:
```
drill+jdbc://admin:password@localhost:31010
```

## Usage with Superset
For a complete tutorial on how to use Superset with Drill, read the tutorial on @cgivre's blog available here: http://thedataist.com/visualize-anything-with-superset-and-drill/.


## Current Status/Development Approach
Currently we can connect to drill, and issue queries for most visualizations and get results. We also enumerate table columns for some times of tables. Here are things that are working as some larger issues to work out. (Individual issues are tracked under issues)

* Connection to Drill via the databases tab in Superset succeeds
* You can do basic queries for most types of viz/tables
* There may be issues with advanced queries/joins. As you learn about new ones, please track in issues

### Many thanks
to drillpy and pydrill for code used in creating the `drilldbapi.py` code for connecting!

### Docker 
Get the superset repo and then in
```
FROM supersetimage(not sure it's name)
RUN git clone https://github.com/JohnOmernik/sqlalchemy-drill && cd sqlalchemy-drill && python3 setup.py install 
CMD["superset"]
```
