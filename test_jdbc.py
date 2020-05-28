# This is the MIT license: http://www.opensource.org/licenses/mit-license.php
#
# Copyright (c) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>.
# SQLAlchemy is a trademark of Michael Bayer.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

from sqlalchemy import create_engine

# It is usually not a good idea to bury starting the JVM in a library.  Doing so would
# make it so that JPype can only be used in the module and not available for other things.
# You would also have to handle cases were the JVM is already started or the JVM was started
# with a different thread than main.
import jpype
#jpype.startJVM("-ea")
jpype.startJVM("-ea", classpath="lib/*")

#engine = create_engine('drill+sadrill://localhost:8047/dfs?use_ssl=False')
#
#with engine.connect() as con:
#    rs = con.execute('SELECT * FROM cp.`employee.json` LIMIT 5')
#    for row in rs:
#        print(row)

print("Now JDBC")
jdbc_engine = create_engine('drill+jdbc://admin:password@localhost:31010')


with jdbc_engine.connect() as con:
    rs = con.execute('SELECT * FROM cp.`employee.json` LIMIT 5')
    for row in rs:
        print(row)
