from sqlalchemy import create_engine

from sqlalchemy_sphinx.dialect import SphinxDialect
from sqlalchemy_sphinx.cymysql import Dialect
from sqlalchemy_sphinx.mysqldb import Dialect as myDialect


def test_cymysql_connection():
    sphinx_engine = create_engine("sphinx+cymsql://")
    assert isinstance(sphinx_engine.dialect, SphinxDialect)
    assert isinstance(sphinx_engine.dialect, Dialect)
    assert sphinx_engine.dialect.escape_value("adri'el") == "'adri\\'el'"


def test_mysqldb_connection():
    sphinx_engine = create_engine("sphinx://")
    assert isinstance(sphinx_engine.dialect, SphinxDialect)
    assert isinstance(sphinx_engine.dialect, myDialect)
    assert sphinx_engine.dialect.escape_value("adri'el") == "adri\\'el"
    sphinx_engine = create_engine("sphinx+mysqldb://")
    assert isinstance(sphinx_engine.dialect, SphinxDialect)
    assert isinstance(sphinx_engine.dialect, myDialect)
    assert sphinx_engine.dialect.escape_value("adri'el") == "adri\\'el"


def test_sanity_on_detects():
    sphinx_engine = create_engine("sphinx+cymsql://")
    sphinx_engine.dialect._get_default_schema_name(None)
    sphinx_engine.dialect._get_server_version_info(None)
    sphinx_engine.dialect._detect_charset(None)
    sphinx_engine.dialect._detect_casing(None)
    sphinx_engine.dialect._detect_collations(None)
    sphinx_engine.dialect._detect_ansiquotes(None)
    sphinx_engine.dialect.get_isolation_level(None)
    sphinx_engine = create_engine("sphinx://")
    sphinx_engine.dialect._get_default_schema_name(None)
    sphinx_engine.dialect._get_server_version_info(None)
    sphinx_engine.dialect._detect_charset(None)
    sphinx_engine.dialect._detect_casing(None)
    sphinx_engine.dialect._detect_collations(None)
    sphinx_engine.dialect._detect_ansiquotes(None)
    sphinx_engine.dialect.get_isolation_level(None)
    sphinx_engine.dialect.do_commit(None)
    sphinx_engine.dialect.do_begin(None)
