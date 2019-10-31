""" CyMySQL connector"""

from __future__ import absolute_import

import cymysql
from cymysql.connections import Connection
from cymysql.converters import ESCAPE_REGEX, ESCAPE_MAP
from sqlalchemy.dialects.mysql import cymysql as cymysql_dialect
from sqlalchemy_sphinx.dialect import SphinxDialect

__all__ = ("Dialect",)


class DBAPIShim(object):

    def connect(self, *args, **kwargs):
        return Connection(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(cymysql, name)


class Dialect(SphinxDialect, cymysql_dialect.MySQLDialect_cymysql):

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
        """cymysql.escape_string without quotes"""
        return "%s" % ESCAPE_REGEX.sub(lambda match: ESCAPE_MAP.get(match.group(0)), value)

    @classmethod
    def dbapi(cls):
        return DBAPIShim()
