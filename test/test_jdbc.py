import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import inspect

engine = create_engine('drill+sadrill://localhost:8047/dfs?use_ssl=False')

with engine.connect() as con:
    rs = con.execute('SELECT * FROM cp.`employee.json` LIMIT 5')
    for row in rs:
        print(row)


print("Now JDBC")
jdbc_engine = create_engine('drill+jdbc://admin:password@localhost:31010')

with jdbc_engine.connect() as con:
    rs = con.execute('SELECT * FROM cp.`employee.json` LIMIT 5')
    for row in rs:
        print(row)