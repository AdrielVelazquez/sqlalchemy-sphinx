import pytest

from sqlalchemy import create_engine

from sqlalchemy_sphinx.dialect import SphinxDialect
from sqlalchemy_sphinx.cymysql import Dialect
from sqlalchemy_sphinx.mysqldb import Dialect as mysqldbDialect
from sqlalchemy_sphinx.pymysql import Dialect as pymysqlDialect


DIALECT_URLS = ("sphinx+cymsql://", "sphinx://", "sphinx+mysqldb://", "sphinx+pymysql://")
DIALECT_CLASSES = (Dialect, mysqldbDialect, mysqldbDialect, pymysqlDialect)
DIALECT_ESCAPE_RESULTS = ("'adri\\'el'", "adri\\'el", "adri\\'el", "adri\\'el")


@pytest.mark.parametrize("url, dialect_class", zip(DIALECT_URLS, DIALECT_CLASSES))
def test_connection(url, dialect_class):
    sphinx_engine = create_engine(url)
    assert isinstance(sphinx_engine.dialect, SphinxDialect)
    assert isinstance(sphinx_engine.dialect, dialect_class)


@pytest.mark.parametrize("url, escape_result", zip(DIALECT_URLS, DIALECT_ESCAPE_RESULTS))
def test_escape(url, escape_result):
    sphinx_engine = create_engine(url)
    assert sphinx_engine.dialect.escape_value("adri'el") == escape_result


@pytest.mark.parametrize("url", DIALECT_URLS)
def test_sanity_on_detects(url):
    sphinx_engine = create_engine(url)
    sphinx_engine.dialect._get_default_schema_name(None)
    sphinx_engine.dialect._get_server_version_info(None)
    sphinx_engine.dialect._detect_charset(None)
    sphinx_engine.dialect._detect_casing(None)
    sphinx_engine.dialect._detect_collations(None)
    sphinx_engine.dialect._detect_ansiquotes(None)
    sphinx_engine.dialect.get_isolation_level(None)
    sphinx_engine.dialect.do_commit(None)
    sphinx_engine.dialect.do_begin(None)
