# -*- coding: utf-8 -*-
import pytest

from sqlalchemy import create_engine, Column, Integer, String, func, distinct, or_, not_, and_, column
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


@pytest.fixture(scope="module")
def MockSphinxModel(sphinx_connections):
    return sphinx_connections[0]


@pytest.fixture(scope="module")
def session(sphinx_connections):
    return sphinx_connections[1]


@pytest.fixture(scope="module")
def sphinx_engine(sphinx_connections):
    return sphinx_connections[2]


@pytest.fixture(scope="function")
def base_query(session, MockSphinxModel):
    return session.query(MockSphinxModel.id)


def func_match(expr, query):
    return func.match(expr, query)


def expression_match(expr, query):
    return expr.match(query)


@pytest.fixture(scope="module", params=[func_match, expression_match])
def match_func(request):
    return request.param


@pytest.fixture(scope="module")
def match_model_name(MockSphinxModel, match_func):
    def match_name(query):
        return match_func(MockSphinxModel.name, query)
    return match_name


@pytest.fixture(scope="module")
def match_model_country(MockSphinxModel, match_func):
    def match_country(query):
        return match_func(MockSphinxModel.country, query)
    return match_country


class TestLimitOffset:
    def test_limit(self, MockSphinxModel, sphinx_engine, session):
        query = session.query(MockSphinxModel).limit(100)
        sql_text = query._compile_context().statement.compile(sphinx_engine).string
        assert sql_text == "SELECT name, id, country \nFROM mock_table\n LIMIT 0, 100"

    def test_limit_and_offset(self, MockSphinxModel, sphinx_engine, session):
        query = session.query(MockSphinxModel).limit(100).offset(100)
        sql_text = query._compile_context().statement.compile(sphinx_engine).string
        assert sql_text == "SELECT name, id, country \nFROM mock_table\n LIMIT %s, %s"


class TestMatch:
    def test_single_column_match(self, sphinx_engine, base_query, match_model_name):
        query = base_query.filter(match_model_name("adriel"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel)')"

    def test_escape_quote(self, sphinx_engine, base_query, match_model_name):
        query = base_query.filter(match_model_name("adri'el"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adri\\'el)')"

    def test_escape_at_symbol(self, sphinx_engine, base_query, match_model_name):
        query = base_query.filter(match_model_name("@username"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name \\\\@username)')"

    def test_escape_multiple_at_symbols(self, sphinx_engine, base_query, match_model_name):
        query = base_query.filter(match_model_name("user @user @name last"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name user \\\\@user \\\\@name last)')"

    def test_escape_brackets(self, sphinx_engine, base_query, match_model_name):
        query = base_query.filter(match_model_name("user )))("))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name user \\\\)\\\\)\\\\)\\\\()')"

    def test_func_match_all(self, sphinx_engine, base_query):
        query = base_query.filter(func.match("adriel"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('adriel')"

    def test_func_match_all_escape_quote(self, sphinx_engine, base_query):
        query = base_query.filter(func.match("adri'el"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('adri\\'el')"

    def test_func_match_all_unicode(self, sphinx_engine, base_query):
        query = base_query.filter(func.match(u"miljøet"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == u"SELECT id \nFROM mock_table \nWHERE MATCH('miljøet')"

    def test_func_match_specific(self, sphinx_engine, base_query):
        query = base_query.filter(func.match("@name adriel"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('@name adriel')"

    def test_func_match_specific_escape_quote(self, sphinx_engine, base_query):
        query = base_query.filter(func.match("@name adri'el"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('@name adri\\'el')"

    def test_func_match_specific_unicode(self, sphinx_engine, base_query):
        query = base_query.filter(func.match(u"@name miljøet"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == u"SELECT id \nFROM mock_table \nWHERE MATCH('@name miljøet')"

    def test_multiple_single_columns_match(self, sphinx_engine, base_query, match_model_name, match_model_country):
        query = base_query.filter(match_model_name("adriel"), match_model_country("US"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel) (@country US)')"

    def test_multiple_single_columns_match_escape_quote(
            self, sphinx_engine, base_query, match_model_name, match_model_country
    ):
        query = base_query.filter(match_model_name("adri'el"), match_model_country("US"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adri\\'el) (@country US)')"

    def test_multiple_single_columns_match_escape_at_symbol(
            self, sphinx_engine, base_query, match_model_name, match_model_country
    ):
        query = base_query.filter(match_model_name("@username"), match_model_country("US"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name \\\\@username) (@country US)')"

    def test_multiple_single_columns_match_escape_multiple_at_symbols(
            self, sphinx_engine, base_query, match_model_name, match_model_country
    ):
        query = base_query.filter(match_model_name("user @user @name"), match_model_country("US"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name user \\\\@user \\\\@name) (@country US)')"

    def test_multiple_single_columns_match_escape_brackets(
            self, sphinx_engine, base_query, match_model_name, match_model_country
    ):
        query = base_query.filter(match_model_name("user )))("), match_model_country("US"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \n" \
                           "WHERE MATCH('(@name user \\\\)\\\\)\\\\)\\\\() (@country US)')"

    def test_multiple_single_columns_match_with_filter(
            self, MockSphinxModel, sphinx_engine, base_query, match_model_name, match_model_country
    ):
        query = base_query.filter(match_model_name("adriel"), match_model_country("US"), MockSphinxModel.id == 1)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel) (@country US)') AND id = %s"

    def test_multiple_single_unicode_columns_match_with_filter(
            self, MockSphinxModel, sphinx_engine, base_query, match_model_name, match_model_country
    ):
        query = base_query.filter(match_model_name(u"miljøet"), match_model_country("US"), MockSphinxModel.id == 1)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == u"SELECT id \nFROM mock_table \nWHERE MATCH('(@name miljøet) (@country US)') AND id = %s"

    def test_ignore_field(self, MockSphinxModel, sphinx_engine, base_query, match_func):
        query = base_query.filter(match_func(not_(MockSphinxModel.country), "US"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@!country US)')"

    def test_multiple_fields(self, MockSphinxModel, sphinx_engine, base_query, match_func):
        query = base_query.filter(match_func(or_(MockSphinxModel.name, MockSphinxModel.country), "US"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@(name,country) US)')"

    def test_ignore_multiple_fields(self, MockSphinxModel, sphinx_engine, base_query, match_func):
        query = base_query.filter(match_func(not_(or_(MockSphinxModel.name, MockSphinxModel.country)), "US"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@!(name,country) US)')"


class TestMatchErrors:
    def test_too_many_arguments_match_func(self, MockSphinxModel, sphinx_engine, base_query):
        with pytest.raises(CompileError):
            base_query.filter(func.match(MockSphinxModel.name, "word1", "word2")).statement.compile(sphinx_engine)

    def test_too_many_arguments_match_expr(self, MockSphinxModel, sphinx_engine, base_query):
        with pytest.raises((CompileError, TypeError)):
            base_query.filter(MockSphinxModel.name.match("word1", "word2")).statement.compile(sphinx_engine)

    def test_too_few_arguments_match_func(self, MockSphinxModel, sphinx_engine, base_query):
        with pytest.raises(CompileError):
            base_query.filter(func.match()).statement.compile(sphinx_engine)

    def test_too_few_arguments_match_expr(self, MockSphinxModel, sphinx_engine, base_query):
        with pytest.raises((CompileError, TypeError)):
            base_query.filter(MockSphinxModel.name.match()).statement.compile(sphinx_engine)

    def test_invalid_argument_match_func(self, MockSphinxModel, sphinx_engine, base_query):
        with pytest.raises(CompileError):
            base_query.filter(MockSphinxModel.name.match(MockSphinxModel.country)).statement.compile(sphinx_engine)

    def test_invalid_argument_match_expr(self, MockSphinxModel, sphinx_engine, base_query):
        with pytest.raises((CompileError, TypeError)):
            base_query.filter(func.match(MockSphinxModel.name, MockSphinxModel.country)).\
                statement.compile(sphinx_engine)

    def test_not_inside_or(self, MockSphinxModel, sphinx_engine, base_query, match_func):
        with pytest.raises(CompileError, match='Invalid source'):
            base_query.filter(match_func(or_(not_(MockSphinxModel.name), MockSphinxModel.country), "US")). \
                statement.compile(sphinx_engine)

    def test_multi_level_or(self, MockSphinxModel, sphinx_engine, base_query, match_func):
        with pytest.raises(CompileError, match='Invalid source'):
            base_query.filter(
                match_func(or_(or_(MockSphinxModel.name, MockSphinxModel.country), MockSphinxModel.name), "US")
            ).statement.compile(sphinx_engine)

    def test_invalid_unary(self, MockSphinxModel, sphinx_engine, base_query, match_func):
        with pytest.raises(CompileError, match='Invalid unary'):
            base_query.filter(match_func(MockSphinxModel.name.asc(), "US")).statement.compile(sphinx_engine)

    def test_invalid_boolean(self, MockSphinxModel, sphinx_engine, base_query, match_func):
        with pytest.raises(CompileError, match='Invalid boolean'):
            base_query.filter(match_func(and_(MockSphinxModel.name, MockSphinxModel.country), "US")).\
                statement.compile(sphinx_engine)

    def test_and_inside_or(self, MockSphinxModel, sphinx_engine, base_query, match_func):
        with pytest.raises(CompileError, match='Invalid boolean'):
            base_query.filter(match_func(not_(and_(MockSphinxModel.name, MockSphinxModel.country)), "US")). \
                statement.compile(sphinx_engine)


class TestSelect:
    def test_visit_column(self, MockSphinxModel, session, sphinx_engine):
        test_literal = column("test_literal", is_literal=True)
        query = session.query(test_literal)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT test_literal \nFROM "

    def test_alias_issue(self, MockSphinxModel, session, sphinx_engine):
        query = session.query(func.sum(MockSphinxModel.id), MockSphinxModel.country)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT sum(id) AS sum_1, country \nFROM mock_table"

    def test_select_from(self, MockSphinxModel, session, sphinx_engine):
        query = session.query(MockSphinxModel.id).select_from(MockSphinxModel)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table"


class TestOptions:
    def test_max_matcher(self, MockSphinxModel, sphinx_engine, base_query):
        query = base_query.filter(func.options(MockSphinxModel.max_matches == 1))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table OPTION max_matches=1"

    def test_field_weights(self, MockSphinxModel, sphinx_engine, base_query):
        query = base_query.filter(func.options(MockSphinxModel.field_weights == ["title=10", "body=3"]))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table OPTION field_weights=(title=10, body=3)"

    def test_match_and_max_matches(self, MockSphinxModel, sphinx_engine, base_query, match_model_country):
        query = base_query.filter(
            match_model_country("US"), func.options(MockSphinxModel.max_matches == 1)
        )
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@country US)') OPTION max_matches=1"


class TestSelectSanity:
    def test_group_by(self, MockSphinxModel, sphinx_engine, base_query, match_model_name):
        query = base_query.filter(match_model_name("adriel")).group_by(MockSphinxModel.country)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel)') GROUP BY country"

    def test_order_by(self, MockSphinxModel, sphinx_engine, base_query, match_model_name):
        query = base_query.filter(match_model_name("adriel")).order_by(MockSphinxModel.country)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT id \nFROM mock_table \nWHERE MATCH('(@name adriel)') ORDER BY country"


class TestAggregation:
    @pytest.fixture(scope="module")
    def count_distinct(self, MockSphinxModel, session):
        return session.query(func.count(distinct(MockSphinxModel.id)))

    @pytest.fixture(scope="module")
    def count_distinct_select(self, MockSphinxModel, session):
        return session.query(func.count(distinct(MockSphinxModel.id)), MockSphinxModel.id)

    @pytest.fixture(scope="module")
    def count_distinct_select_sum(self, MockSphinxModel, session):
        return session.query(func.count(distinct(MockSphinxModel.id)), MockSphinxModel.id, func.sum(MockSphinxModel.id))

    def test_count(self, MockSphinxModel, sphinx_engine, session):
        query = session.query(func.count(MockSphinxModel.id))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT COUNT(*) AS count_1 \nFROM mock_table"

    def test_count_match(self, MockSphinxModel, sphinx_engine, session):
        query = session.query(func.count('*')).select_from(MockSphinxModel).filter(func.match("adriel"))
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT COUNT(*) AS count_1 \nFROM mock_table \nWHERE MATCH('adriel')"

    def test_count_distinct(self, sphinx_engine, count_distinct):
        sql_text = count_distinct.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT COUNT(DISTINCT id) AS count_1 \nFROM mock_table"

    def test_count_distinct_group_by(self, MockSphinxModel, sphinx_engine, count_distinct):
        query = count_distinct.group_by(MockSphinxModel.group_by_dummy)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT COUNT(DISTINCT id) AS count_1 \nFROM mock_table GROUP BY group_by_dummy"

    def test_count_distinct_select(self, sphinx_engine, count_distinct_select):
        sql_text = count_distinct_select.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT COUNT(DISTINCT id) AS count_1, id \nFROM mock_table"

    def test_count_distinct_select_group_by(self, MockSphinxModel, sphinx_engine, count_distinct_select):
        query = count_distinct_select.group_by(MockSphinxModel.group_by_dummy)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT COUNT(DISTINCT id) AS count_1, id \nFROM mock_table GROUP BY group_by_dummy"

    def test_count_distinct_select_sum(self, sphinx_engine, count_distinct_select_sum):
        sql_text = count_distinct_select_sum.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT COUNT(DISTINCT id) AS count_1, id, sum(id) AS sum_1 \nFROM mock_table"

    def test_count_distinct_select_sum_group_by(self, MockSphinxModel, sphinx_engine, count_distinct_select_sum):
        query = count_distinct_select_sum.group_by(MockSphinxModel.group_by_dummy)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT COUNT(DISTINCT id) AS count_1, id, sum(id) AS sum_1 \n" \
                           "FROM mock_table GROUP BY group_by_dummy"

    def test_avg_func(self, MockSphinxModel, sphinx_engine, session):
        query = session.query(func.avg(MockSphinxModel.id))
        query = query.group_by(MockSphinxModel.group_by_dummy)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT avg(id) AS avg_1 \nFROM mock_table GROUP BY group_by_dummy"

    def test_min_func(self, MockSphinxModel, sphinx_engine, session):
        query = session.query(func.min(MockSphinxModel.id))
        query = query.group_by(MockSphinxModel.group_by_dummy)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT min(id) AS min_1 \nFROM mock_table GROUP BY group_by_dummy"

    def test_max_func(self, MockSphinxModel, sphinx_engine, session):
        query = session.query(func.max(MockSphinxModel.id))
        query = query.group_by(MockSphinxModel.group_by_dummy)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT max(id) AS max_1 \nFROM mock_table GROUP BY group_by_dummy"

    def test_groupby_func(self, MockSphinxModel, sphinx_engine, session):
        query = session.query(func.avg(MockSphinxModel.id), func.groupby())
        query = query.group_by(MockSphinxModel.group_by_dummy)
        sql_text = query.statement.compile(sphinx_engine).string
        assert sql_text == "SELECT avg(id) AS avg_1, groupby() AS groupby_1 \nFROM mock_table GROUP BY group_by_dummy"
