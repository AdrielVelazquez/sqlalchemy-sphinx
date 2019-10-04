import pytest

from sqlalchemy import create_engine

from sqlalchemy_sphinx.dialect import SphinxDialect
from sqlalchemy_sphinx.cymysql import Dialect as cymysqlDialect
from sqlalchemy_sphinx.mysqldb import Dialect as mysqldbDialect
from sqlalchemy_sphinx.pymysql import Dialect as pymysqlDialect


@pytest.fixture(scope="module", params=(
    "sphinx://", "sphinx+pymysql://", "sphinx+mysqldb://", "sphinx+cymysql://"
))
def connection_url(request):
    return request.param


@pytest.fixture(scope="module")
def dialect_class(connection_url):
    if "cymysql" in connection_url:
        return cymysqlDialect
    elif "mysqldb" in connection_url:
        return mysqldbDialect
    elif "pymysql" in connection_url:
        return pymysqlDialect
    else:
        return mysqldbDialect


@pytest.fixture(scope="module")
def sphinx_engine(connection_url):
    return create_engine(connection_url)


def test_connection(sphinx_engine, dialect_class):
    assert isinstance(sphinx_engine.dialect, SphinxDialect)
    assert isinstance(sphinx_engine.dialect, dialect_class)


def test_escape(sphinx_engine):
    assert sphinx_engine.dialect.escape_value("adri'el") == "adri\\'el"


def test_sanity_on_detects(sphinx_engine):
    sphinx_engine.dialect._get_default_schema_name(None)
    sphinx_engine.dialect._get_server_version_info(None)
    sphinx_engine.dialect._detect_charset(None)
    sphinx_engine.dialect._detect_casing(None)
    sphinx_engine.dialect._detect_collations(None)
    sphinx_engine.dialect._detect_ansiquotes(None)
    sphinx_engine.dialect.get_isolation_level(None)
    sphinx_engine.dialect.do_commit(None)
    sphinx_engine.dialect.do_begin(None)
