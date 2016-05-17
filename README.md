# Apache Drill dialect for SQLAlchemy.

## Dependencies
```
virtualenv env
. env/bin/activate
pip install -r requirements/test.txt
```

## Running tests
```
python run_tests.py -v -s
```

Debugging is enabled by inserting ```import pdb; pdb.set_trace()```


### FAQ

Q: I get error of "[IM002] [unixODBC][Driver Manager]Data source name not found, and no default driver specified"

A: Inspect setup.cfg, in section of [db] it will try to connect to default. 
In mac os x you might need to change ~/Library/ODBC/odbc.ini or ~/.odbc.ini with s/Sample MapR Drill DSN/drill_test/.
