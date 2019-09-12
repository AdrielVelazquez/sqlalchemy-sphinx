""" MySQLdb connector"""

from __future__ import absolute_import

import MySQLdb
from sqlalchemy.dialects.mysql import mysqldb as mysqldb_dialect
from sqlalchemy_sphinx.dialect import SphinxDialect

__all__ = ("Dialect",)


class DBAPIShim(object):

    def connect(self, *args, **kwargs):
        return MySQLdb.Connection(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(MySQLdb, name)


class Dialect(SphinxDialect, mysqldb_dialect.MySQLDialect_mysqldb):

    def _get_default_schema_name(self, connection):
        """Prevent 'SELECT DATABASE()' being executed"""
        return None

    def _get_server_version_info(self, connection):
        """Prevent 'SELECT VERSION()' being executed. Return empty tuple for compatibility"""
        return tuple()

    def _detect_charset(self, connection):
        pass

    def _detect_casing(self, connection):
        pass

    def _detect_collations(self, connection):
        pass

    def _detect_ansiquotes(self, connection):
        self._server_ansiquotes = False

    def get_isolation_level(self, connection):
        pass

    def escape_value(self, value):
        value = MySQLdb.escape_string(value.encode('utf8'))
        return value.decode('utf8')

    @classmethod
    def dbapi(cls):
        return DBAPIShim()
