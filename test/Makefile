.ONESHELL:
.PHONY: setup

.EXPORT_ALL_VARIABLES:
PIP_REQUIRE_VIRTUALENV = true

SHELL := /bin/bash

setup:
	rm -rf test-env
	python3 -m venv test-env
	source test-env/bin/activate && pip install -r requirements.txt

unit-test:
	source test-env/bin/activate && pytest -svx test_unit.py
