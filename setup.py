import sys

from setuptools import setup
from setuptools.command.test import test as TestCommand


class Tox(TestCommand):
    user_options = [('tox-args=', 'a', "Arguments to pass to tox")]
    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.tox_args = None
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import tox
        import shlex
        args = self.tox_args
        if args:
            args = shlex.split(self.tox_args)
        errno = tox.cmdline(args=args)
        sys.exit(errno)

setup(
    name="sqlalchemy_sphinx",
    version="0.8.3",
    description="SQLAlchemy extension for dealing with SphinxQL",
    long_description=open("README.rst", "r").read(),
    author="Adriel Velazquez",
    author_email="adrielvelazquez@gmail.com",
    packages=['sqlalchemy_sphinx'],
    cmdclass = {'test': Tox},
    zip_safe=False,
    install_requires=[
        "sqlalchemy > 0.9",
    ],
    tests_require=['tox'],
    entry_points={
     'sqlalchemy.dialects': [
          'sphinx = sqlalchemy_sphinx.mysqldb:Dialect',
          'sphinx.cymysql = sqlalchemy_sphinx.cymysql:Dialect',
          'sphinx.mysqldb = sqlalchemy_sphinx.mysqldb:Dialect'
          ]
    }
)
