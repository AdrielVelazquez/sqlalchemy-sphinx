SHELL := /bin/bash
update:
	pyvenv-3.6 env && source env/bin/activate && pip install tox

clean-pycs:
	find . -name '*.pyc' -delete

clean-cache:
	find . | grep -E "(__pycache__|\.cache)" | xargs rm -rf


test: update clean-pycs clean-cache
	source env/bin/activate && tox

test-docker-27:
	pip install tox && tox -e "py27-sqlalchemy{10,11,12,13}"

test-docker-36:
	pip install tox && tox -e "py36-sqlalchemy{10,11,12,13}"

