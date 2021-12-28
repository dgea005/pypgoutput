SHELL := /bin/bash

.EXPORT_ALL_VARIABLES:
PIP_REQUIRE_VIRTUALENV = true

VENV?=test/test-env
INSTALL_STAMP := $(VENV)/.install.stamp
PYTHON=${VENV}/bin/python3

venv: ${VENV}/bin/activate

$(VENV)/bin/activate: setup.py test/requirements-dev.txt
	if [ ! -d $(VENV) ]; then python3 -m venv $(VENV); fi
	${PYTHON} -m pip install --upgrade pip
	${PYTHON} -m pip install -r test/requirements-dev.txt
	${PYTHON} -m pip install -e .
	touch $(INSTALL_STAMP)

.PHONY: test
test: venv
	${PYTHON} -m pytest -vv test/test_unit.py

.PHONY: check-format
check-format: venv
	${PYTHON} -m black -t py38 --check ./src


clean:
	rm -rf $(VENV)
	find . -type f -name *.pyc -delete
	find . -type d -name __pycache__ -delete
