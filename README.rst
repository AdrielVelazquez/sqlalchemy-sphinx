SQLAlchemy Sphinx
=================

|travis| |coveralls|

.. |travis| image:: https://travis-ci.org/AdrielVelazquez/sqlalchemy-sphinx.svg?branch=master
    :target: https://travis-ci.org/AdrielVelazquez/sqlalchemy-sphinx
    
.. |pypi| image:: https://badge.fury.io/py/sqlalchemy-sphinx.svg
    :target: https://badge.fury.io/py/sqlalchemy-sphinx
    
.. |coveralls| image:: https://coveralls.io/repos/github/AdrielVelazquez/sqlalchemy-sphinx/badge.svg?branch=master 
    :target: https://coveralls.io/github/AdrielVelazquez/sqlalchemy-sphinx?branch=master

SQLAlchemy Sphinx is a dialect for SQLalchemy which converts SQLAlchemy
model into compatible sql for sphinx.

This dialect works for both python 2 and 3. Currently you need to import
sqlalchemy\_sphinx to properly register for python 3.

This fork adds support for Python 2.6

Installation
------------

SQLAlchemy Sphinx is available on pypi under the package name
``sqlalchemy-sphinx``, you can get it by running:

.. code:: sh

    pip install sqlalchemy-sphinx

Usage
-----

Defining a Sphinx SQLAlchemy is exactly the same way you would create a
sqlalchemy model.

.. code:: python


    from sqlalchemy import create_engine, Column, Integer, String, BigInteger, Unicode, Enum
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import deferred, sessionmaker

    sphinx_engine = create_engine('sphinx://your.sphinx.host:9008')
    SphinxBase = declarative_base(bind=sphinx_engine)
    SphinxSession = sessionmaker(bind=sphinx_engine)
    sphinx_session = SphinxSession()


    class MockSphinxModel(Base):
        __tablename__ = "mock_table"
        name = Column(String)
        id = Column(Integer, primary_key=True)
        country = Column(String)
        ranker = deferred(Column(String))
        group_by_dummy = deferred(Column(String))
        max_matches = deferred(Column(String))
        field_weights = deferred(Column(String))

After the model is created we can run queries against the model:

.. code:: python

    query = session.query(MockSphinxModel).limit(100)
    # 'SELECT name, id, country FROM mock_table LIMIT 0, 100'

We can also do matching

.. code:: python

    base_query = session.query(MockSphinxModel.id)
    query = base_query.filter(MockSphinxModel.country.match("US"))
    # "SELECT id FROM mock_table WHERE MATCH('(@country US)')"

    query = base_query.filter(MockSphinxModel.name.match("adriel"), MockSphinxModel.country.match("US"))
    # "SELECT id FROM mock_table WHERE MATCH('(@name adriel) (@country US)')"

Options:

.. code:: python

    query = session.query(MockSphinxModel.id)
    query = query.filter(func.options(MockSphinxModel.field_weights == ["title=10", "body=3"]))
    # 'SELECT id FROM mock_table OPTION field_weights=(title=10, body=3)'

    query = session.query(MockSphinxModel.id)
    query = query.filter(MockSphinxModel.country.match("US"), func.options(MockSphinxModel.max_matches == 1))
    # "SELECT id FROM mock_table WHERE MATCH('(@country US)') OPTION max_matches=1"
