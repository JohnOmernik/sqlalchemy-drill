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

from __future__ import absolute_import
from __future__ import unicode_literals
import logging

import pyodbc

from .base import DrillDialect, DrillCompiler_sadrill

logger = logging.getLogger(__name__)


class DrillDialect_odbc(DrillDialect):
    statement_compiler = DrillCompiler_sadrill

    def create_connect_args(self, url, **kwargs):
        if url is None:
            return [], {}

        params = super(DrillDialect, self).create_connect_args(url)[1]

        # Convert URL query parameters to ODBC connection string
        connection_string = ";".join(f"{k}={v}" for k, v in params.items())

        cargs = (connection_string,)
        cparams = {
            "autocommit": True,  # Drill ODBC driver does not support transactions
        }

        logger.info(f"Cargs: {cargs}")
        logger.info(f"Cparams: {cparams}")

        return cargs, cparams

    @classmethod
    def dbapi(cls):
        return pyodbc


dialect = DrillDialect_odbc
