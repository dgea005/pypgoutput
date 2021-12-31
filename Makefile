SHELL := /bin/bash

.EXPORT_ALL_VARIABLES:
PIP_REQUIRE_VIRTUALENV = true

# INSTALL_STAMP is from here
# https://blog.mathieu-leplatre.info/tips-for-your-makefile-with-python.html

VENV?=dev-venv
INSTALL_STAMP := $(VENV)/.install.stamp
PYTHON=${VENV}/bin/python3

venv: $(INSTALL_STAMP)

$(INSTALL_STAMP): setup.py tests/requirements-dev.txt
	if [ ! -d $(VENV) ]; then python3 -m venv $(VENV); fi
	${PYTHON} -m pip install --upgrade pip
	${PYTHON} -m pip install -r tests/requirements-dev.txt
	${PYTHON} -m pip install -e .
	touch $(INSTALL_STAMP)

.PHONY: test
test: venv
	${PYTHON} -m pytest -vv tests/test_unit.py
	${PYTHON} -m pytest -svx tests/test_integration.py

.PHONY: test-local
test-local: venv
	${PYTHON} -m pytest -vv tests/test_unit.py
	./tests/start_local_db.sh
	${PYTHON} -m pytest -svx tests/test_integration.py

.PHONY: check-format
check-format: venv
	${PYTHON} -m black --config=pyproject.toml --check src/ tests/

.PHONY: format
format: venv
	${PYTHON} -m isort src/ tests/
	${PYTHON} -m black --config=pyproject.toml src/ tests/

.PHONY: lint
lint: venv
	${PYTHON} -m flake8 --ignore=W503,E501 src/ tests/
	${PYTHON} -m isort src/ tests/ --check-only
	${PYTHON} -m black --config=pyproject.toml src/ tests/ --check


.PHONY: clean
clean:
	rm -rf $(VENV)
	find . -type f -name *.pyc -delete
	find . -type d -name __pycache__ -delete
