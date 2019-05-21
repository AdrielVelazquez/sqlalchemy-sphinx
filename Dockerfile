FROM python:3.6

ADD . /sphinxTests
WORKDIR /sphinxTests
RUN pip install tox
