from sqlalchemy.dialects import registry

__version__ = "0.8"

# https://bitbucket.org/zzzeek/sqlalchemy/issues/3536/engine-entrypoint-plugins
registry.register("sphinx", "sqlalchemy_sphinx.mysqldb", "Dialect")
registry.register("sphinx.cymsql", "sqlalchemy_sphinx.cymysql", "Dialect")
registry.register("sphinx.mysqldb", "sqlalchemy_sphinx.mysqldb", "Dialect")
registry.register("sphinx.pymysql", "sqlalchemy_sphinx.pymysql", "Dialect")
