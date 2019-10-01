# -*- coding: utf-8 -*-
import pytest

from sqlalchemy import create_engine, Column, Integer, String, func, distinct, or_, not_, and_
from sqlalchemy.orm import sessionmaker, deferred
from sqlalchemy.exc import CompileError
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
    base_query = session.query(MockSphinxModel.id)

    # One Match
    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.name.match("adriel"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel)')"

    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, "adriel"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel)')"

    # Escape quote
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, "adri'el"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adri\\'el)')"

    # Escape at symbol
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, "@username"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name \\\\@username)')"

    # Escape multiple at symbols
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, "user @user @name"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name user \\\\@user \\\\@name)')"

    # Escape brackets
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, "user )))("))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name user \\\\)\\\\)\\\\)\\\\()')"

    # Function match all
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match("adriel"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('adriel')"

    # Function match all with quote
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match("adri'el"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('adri\\'el')"

    # Function match all with unicode
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(u"miljøet"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == u"SELECT id \nFROM mock_table \nWHERE MATCH('miljøet')"

    # Function match specific
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match("@name adriel"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('@name adriel')"

    # Function match specific with quote
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match("@name adri'el"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('@name adri\\'el')"

    # Function match specific with unicode
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(u"@name miljøet"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == u"SELECT id \nFROM mock_table \nWHERE MATCH('@name miljøet')"

    # Matching single columns
    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.name.match("adriel"), MockSphinxModel.country.match("US"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel) (@country US)')"

    # Matching single columns with quotes
    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.name.match("adri'el"), MockSphinxModel.country.match("US"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adri\\'el) (@country US)')"

    # Matching single columns with at symbol
    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.name.match("@username"), MockSphinxModel.country.match("US"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name \\\\@username) (@country US)')"

    # Matching single columns with multiple at symbols
    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.name.match("user @user @name"), MockSphinxModel.country.match("US"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name user \\\\@user \\\\@name) (@country US)')"

    # Matching single columns with brackets
    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.name.match("user )))("), MockSphinxModel.country.match("US"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name user \\\\)\\\\)\\\\)\\\\() (@country US)')"

    # Matching through functions
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, "adriel"), func.match(MockSphinxModel.country, "US"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel) (@country US)')"

    # Matching with not_
    base_expression = not_(MockSphinxModel.country)
    for expression in (base_expression.match("US"), func.match(base_expression, "US")):
        query = base_query.filter(expression)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@!country US)')"

    # Matching multiple columns with or_
    base_expression = or_(MockSphinxModel.name, MockSphinxModel.country)
    for expression in (base_expression.match("US"), func.match(base_expression, "US")):
        query = base_query.filter(expression)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@(name,country) US)')"

    # Matching multiple columns with or_ and not_ through functions
    base_expression = not_(or_(MockSphinxModel.name, MockSphinxModel.country))
    for expression in (base_expression.match("US"), func.match(base_expression, "US")):
        query = base_query.filter(expression)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@!(name,country) US)')"

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

    # Match with normal filter with unicode
    query = session.query(MockSphinxModel.id)
    query = query.filter(func.match(MockSphinxModel.name, u"miljøet"), MockSphinxModel.country.match("US"),
        MockSphinxModel.id == 1)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == u"SELECT id \nFROM mock_table \nWHERE MATCH('(@name miljøet) (@country US)') AND id = %s"

    query = session.query(MockSphinxModel.id)
    query = query.filter(func.random(MockSphinxModel.name))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT id \nFROM mock_table \nWHERE random(name)"


def test_match_errors(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections
    query = session.query(MockSphinxModel.id)

    with pytest.raises(CompileError):
        query.filter(func.match(MockSphinxModel.name, "word1", "word2")).statement.compile(sphinx_engine)

    with pytest.raises(CompileError):
        query.filter(func.match()).statement.compile(sphinx_engine)

    # not_ inside or_
    with pytest.raises(CompileError, match='Invalid source'):
        query.filter(or_(not_(MockSphinxModel.name), MockSphinxModel.country).match("US")).\
            statement.compile(sphinx_engine)

    # multi level or
    with pytest.raises(CompileError, match='Invalid source'):
        query.filter(or_(or_(MockSphinxModel.name, MockSphinxModel.country), MockSphinxModel.name).match("US")).\
            statement.compile(sphinx_engine)

    # invalid unary
    with pytest.raises(CompileError, match='Invalid unary'):
        query.filter(MockSphinxModel.name.asc().match("US")).\
            statement.compile(sphinx_engine)

    # and_
    with pytest.raises(CompileError, match='Invalid boolean'):
        query.filter(and_(MockSphinxModel.name, MockSphinxModel.country).match("US")).statement.compile(sphinx_engine)

    # and_ inside not_
    with pytest.raises(CompileError, match='Invalid boolean'):
        query.filter(not_(and_(MockSphinxModel.name, MockSphinxModel.country)).match("US")).\
            statement.compile(sphinx_engine)


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


def test_count(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections

    query = session.query(func.count(MockSphinxModel.id))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT COUNT(*) AS count_1 \nFROM mock_table'

    query = session.query(func.count('*')).select_from(MockSphinxModel).filter(func.match("adriel"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT COUNT(*) AS count_1 \nFROM mock_table \nWHERE MATCH('adriel')"


def test_distinct_and_count(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections

    query = session.query(func.count(distinct(MockSphinxModel.id)))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT COUNT(DISTINCT id) AS count_1 \nFROM mock_table'
    query = query.group_by(MockSphinxModel.group_by_dummy)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT COUNT(DISTINCT id) AS count_1 \nFROM mock_table GROUP BY group_by_dummy'

    query = session.query(func.count(distinct(MockSphinxModel.id)), MockSphinxModel.id)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT COUNT(DISTINCT id) AS count_1, id \nFROM mock_table'
    query = query.group_by(MockSphinxModel.group_by_dummy)
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == 'SELECT COUNT(DISTINCT id) AS count_1, id \nFROM mock_table GROUP BY group_by_dummy'

    query = session.query(func.count(distinct(MockSphinxModel.id)), MockSphinxModel.id, func.sum(MockSphinxModel.id))
    st = query.statement.compile(sphinx_engine).string
    assert st == 'SELECT COUNT(DISTINCT id) AS count_1, id, sum(id) AS sum_1 \nFROM mock_table'
    query = query.group_by(MockSphinxModel.group_by_dummy)
    st = query.statement.compile(sphinx_engine).string
    assert st == 'SELECT COUNT(DISTINCT id) AS count_1, id, sum(id) AS sum_1 \nFROM mock_table GROUP BY group_by_dummy'


def test_result_maps_configurations(sphinx_connections):
    MockSphinxModel, session, sphinx_engine = sphinx_connections
    query = session.query(func.count('*')).select_from(MockSphinxModel).filter(func.match("adriel"))
    sql_text = query.statement.compile(sphinx_engine).string
    assert sql_text == "SELECT COUNT(*) AS count_1 \nFROM mock_table \nWHERE MATCH('adriel')"
