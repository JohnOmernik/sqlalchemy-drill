# This is the MIT license: http://www.opensource.org/licenses/mit-license.php
#
# Copyright (c) 2005-2012 the SQLAlchemy authors and contributors <see AUTHORS file>.
# SQLAlchemy is a trademark of Michael Bayer.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the 'Software'), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons
# to whom the Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or
# substantial portions of the Software.

# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
# PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
# FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
# OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals
import logging
from urllib.parse import unquote

from sqlalchemy import pool

from .base import DrillDialect, DrillIdentifierPreparer, DrillCompiler_sadrill

logger = logging.getLogger('sadrill')


class DrillDialect_sadrill(DrillDialect):

    name = 'drilldbapi'
    driver = 'rest'
    preparer = DrillIdentifierPreparer
    statement_compiler = DrillCompiler_sadrill
    poolclass = pool.SingletonThreadPool
    supports_alter = False
    supports_pk_autoincrement = False
    supports_default_values = False
    supports_empty_insert = False
    supports_unicode_statements = True
    supports_unicode_binds = True
    returns_unicode_strings = True
    description_encoding = None
    supports_native_boolean = True

    def __init__(self, **kw):
        super().__init__(**kw)
        self.supported_extensions = []
        # Initialize attributes that will be set in create_connect_args
        self.host = None
        self.port = None
        self.username = None
        self.password = None
        self.db = None
        self.storage_plugin = None
        self.workspace = None

    @classmethod
    def import_dbapi(cls):
        import sqlalchemy_drill.drilldbapi as module  # pylint: disable=import-outside-toplevel
        return module

    @classmethod
    def dbapi(cls):
        """Deprecated in SQLAlchemy, retained for backwards compatibility."""
        return DrillDialect_sadrill.import_dbapi()

    def create_connect_args(self, url, **kwargs):
        url_port = url.port or 8047
        qargs = {'host': url.host, 'port': url_port}

        try:
            # URL-decode the database path to handle encoded characters like %2F -> /
            raw_database = unquote(url.database) if url.database else 'drill'
            db_parts = raw_database.split('/')
            db = '.'.join(db_parts)

            # Save this for later use.
            self.host = url.host
            self.port = url_port
            self.username = url.username
            self.password = url.password
            self.db = db

            # Get Storage Plugin Info:
            if db_parts[0]:
                self.storage_plugin = db_parts[0]

            if len(db_parts) > 1:
                self.workspace = db_parts[1]

            qargs.update(url.query)
            qargs['db'] = db

            # Convert stream_results to boolean if present
            if 'stream_results' in qargs:
                qargs['stream_results'] = qargs['stream_results'] in [True, 'True', 'true', '1']

            if url.username:
                qargs['drilluser'] = url.username
                qargs['drillpass'] = ''
                if url.password:
                    qargs['drillpass'] = url.password
        except Exception as ex:
            logger.error('could not parse the provided connection url.')
            raise ex

        return [], qargs
