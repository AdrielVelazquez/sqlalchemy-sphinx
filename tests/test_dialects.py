import sys

import pytest

from sqlalchemy import create_engine, Column, Integer, String, func, distinct
from sqlalchemy.orm import sessionmaker, deferred
from sqlalchemy.ext.declarative import declarative_base


@pytest.fixture(scope="module")
def sphinx_connections():
    sphinx_engine = create_engine("sphinx://")
    Base = declarative_base(bind=sphinx_engine)
    Session = sessionmaker(bind=sphinx_engine)
    session = Session()

    class MockSphinxModel(Base):
        __tablename__ = "mock_table"
        name = Column(String)
        id = Column(Integer, primary_key=True)
        country = Column(String)
        ranker = deferred(Column(String))
        group_by_dummy = deferred(Column(String))
        max_matches = deferred(Column(String))
        field_weights = deferred(Column(String))

    return MockSphinxModel, session, sphinx_engine


def test_limit_and_offset(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections
    query = session.query(MockSphinxModel).limit(100)
    assert query.statement.compile(sphinx_engine).string == 'SELECT name, id, country \nFROM mock_table\n LIMIT 0, 100'
    query = session.query(MockSphinxModel).limit(100).offset(100)
    assert query.statement.compile(sphinx_engine).string == 'SELECT name, id, country \nFROM mock_table\n LIMIT %s, %s'


def test_match(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections

    # One Match
    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.name.match("adriel"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel)')"

    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, "adriel"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel)')"

    # Matching single columns
    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.name.match("adriel"), MockSphinxModel.country.match("US"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel) (@country US)')"

    # Matching through functions
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, "adriel"), func.match(MockSphinxModel.country, "US"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel) (@country US)')"

    # Mixing and Matching
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, "adriel"), MockSphinxModel.country.match("US"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel) (@country US)')"

    # Match with normal filter
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, "adriel"), MockSphinxModel.country.match("US"),
        MockSphinxModel.id == 1)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel) (@country US)') AND id = %s"

    query = session.query(MockSphinxModel.id)
    query = query.filter(func.random(MockSphinxModel.name))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE random(name)"


def test_visit_column(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections

    from sqlalchemy import column
    test_literal = column("test_literal", is_literal=True)

    query = session.query(test_literal)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT test_literal \nFROM '


def test_alias_issue(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections
    query = session.query(func.sum(MockSphinxModel.id), MockSphinxModel.country)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT sum(id) AS sum_1, country \nFROM mock_table'


def test_options(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.options(MockSphinxModel.max_matches == 1))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT id \nFROM mock_table OPTION max_matches=1'

    query = session.query(MockSphinxModel.id)
    query = query.filter(func.options(MockSphinxModel.field_weights == ["title=10", "body=3"]))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT id \nFROM mock_table OPTION field_weights=(title=10, body=3)'

    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.country.match("US"), func.options(MockSphinxModel.max_matches == 1))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@country US)') OPTION max_matches=1"


def test_select_sanity(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections

    # Test Group By
    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.name.match("adriel")).group_by(MockSphinxModel.country)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel)') GROUP BY country"

    # Test Order BY
    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.name.match("adriel")).order_by(MockSphinxModel.country)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel)') ORDER BY country"


def test_distinct_and_count(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections
    query = session.query(distinct(MockSphinxModel.name)).group_by(MockSphinxModel.group_by_dummy)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT DISTINCT name AS anon_1 \nFROM mock_table GROUP BY group_by_dummy'
    query = session.query(func.count(distinct(MockSphinxModel.id))).group_by(MockSphinxModel.group_by_dummy)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT count(DISTINCT id) AS count_1 \nFROM mock_table GROUP BY group_by_dummy'
    query = session.query(func.distinct(MockSphinxModel.name)).group_by(MockSphinxModel.group_by_dummy)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT DISTINCT name AS distinct_1 \nFROM mock_table GROUP BY group_by_dummy'
    query = session.query(func.count(distinct(MockSphinxModel.id)), MockSphinxModel.id)
    query = query.group_by(MockSphinxModel.group_by_dummy)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT count(DISTINCT id) AS count_1, id \nFROM mock_table GROUP BY group_by_dummy'
    query = session.query(func.count(distinct(MockSphinxModel.id)), MockSphinxModel.id, func.sum(MockSphinxModel.id))
    query = query.group_by(MockSphinxModel.group_by_dummy)
    st = query.statement.compile(sphinx_engine).string
    assert st == 'SELECT count(DISTINCT id) AS count_1, id, sum(id) AS sum_1 \nFROM mock_table GROUP BY group_by_dummy'


def test_result_maps_configurations(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections
    with pytest.raises(AssertionError) as exc:
        query = session.query(func.count(distinct(MockSphinxModel.country)))
        query.statement.compile(sphinx_engine).string
    if sys.version_info.major >= 3:
        assert str(exc.value) == "Can't query distinct if no group by  is selected"
    else:
        assert exc.value.message == "Can't query distinct if no group by  is selected"
