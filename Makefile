MYPY_OPTIONS = --ignore-missing-imports --disallow-untyped-calls --disallow-untyped-defs --disallow-incomplete-defs

install:
	python -m ipykernel install --user --name luigi

.PHONY: integration-test
integration-test:
	poetry run pytest tests/integration

.PHONY: unit-test
unit-test:
	poetry run pytest tests/unit

.PHONY: lint-check
lint-check:
	poetry run pylint workflow api tests

.PHONY: type-check
type-check:
	poetry run mypy ${MYPY_OPTIONS} workflow api tests

.PHONY: style-checks
style-checks: lint-check type-check

.PHONY: tests
tests: unit-test integration-test

requirements.txt:
	poetry export -f requirements.txt --output requirements.txt --dev
