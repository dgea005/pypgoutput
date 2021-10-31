.ONESHELL:
.PHONY: setup

.EXPORT_ALL_VARIABLES:
PIP_REQUIRE_VIRTUALENV = true

SHELL := /bin/bash

setup:
	rm -rf test-env
	python3 -m venv test-env
	source test-env/bin/activate && pip install -r requirements.txt

test-all:
	source test-env/bin/activate && pytest -svx test_unit.py
	bash ./run_integration_tests.sh

test-unit:
	source test-env/bin/activate && pytest -svx test_unit.py

test-integration:
	bash ./run_integration_tests.sh

