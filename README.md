# Apache Drill dialect for SQLAlchemy.
```
The primary purpose of this is to have a working dialect for Apache Drill that can be used with Caravel 

https://github.com/airbnb/caravel

Obviously, a working, robust dialect for Drill serves other purposes as well, but most of the iterative planning for this REPO will be based on working with Caravel. Other changes will gladly be incorporated, as long as it doesn't hurt caravel integration. 

## Current Status/Development Approach
Currently we can connect to drill, and issue queries for basic visualizations and get results. We also ennumerate table columns for some times of tables. Here are things that are working as some larger issues to work out. (Individual issues are tracked under issues)

* Connection to Drill via the databases tab in Caravel succeeeds
  * You need to specify schema in your connection string because caravel lists tables and will error if you don't have a default schema identified. Tracked in issue: https://github.com/JohnOmernik/sqlalchemy-drill/issues/4
  * Table lists in the connection for some reason only show the first two qualifiers of drill tables.  So if in the schema dfs.root you have two tables, table1 and table2, instead of showing table1 and table2 or dfs.root.table1 and dfs.root.table2, it will just show dfs.root and dfs.root.  This is tracked in issue: https://github.com/JohnOmernik/sqlalchemy-drill/issues/3
* You can add tables under the tables menu
  * Columns are listed properly from the table - However the their types are not all are returned as VARCHAR Tracked here: https://github.com/JohnOmernik/sqlalchemy-drill/issues/10
* You can do basic queries for certain types of viz/tables
  * More advanced queries/joins appear to have issues. As you learn about new ones, please track in issues

So, we are "limping along" and working as is, but contribution and just testing/use to identify issues would be very welcome! 




## Dependencies
There are a couple of ways to approach using/developing on this: 

```
### Docker Approach
While using virtualenv is typically prefered, there is a docker container that works at 

https://github.com/JohnOmernik/caraveldrill

This Dockerfile will build a container has caravel, pyodbc, unixodbc, the MapR Drill ODBC connector all installed ready to go.  In fact all one has to do is set a directory to this repo, and then from within the container, execute:

python /path/to/thisrepo/setup.py install

And this dialect will be installed and ready to go.  

### virualenv 

virtualenv env
. env/bin/activate
pip install -r requirements/test.txt
```

## Running tests
```
python run_tests.py -v -s
```

Debugging is enabled by inserting ```import pdb; pdb.set_trace()```

Also, if using caravel, updating the caravel_config.py to uncomment the debug line will also assist in debugging from caravel 

## FAQ

Q: I get error of "[IM002] [unixODBC][Driver Manager]Data source name not found, and no default driver specified"

A: Inspect setup.cfg, in section of [db] it will try to connect to default. 

In mac os x you might need to change ~/Library/ODBC/odbc.ini or ~/.odbc.ini with s/Sample MapR Drill DSN/drill_test/.

Q: Why is there all that access stuff in the dialect?

A: Because in learning how to do a dialect, we copied the access pyodbc dialect. It's not ideal, and once we figure that a certain function/class isn't needed for drill, or can be replaced by a drill specific one, that is our goal. 


