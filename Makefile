.EXPORT_ALL_VARIABLES:

ifneq (,$(wildcard ./.env))
    include .env
    export
endif

FULLNAME=${PROJECT_NAME}-${PROJECT_VERSION}
PACKAGE_NAME=${FULLNAME}-py3-none-any.whl

default: isset-PROJECT_NAME
	@echo "Tasks in \033[1;32m${PROJECT_NAME}\033[0m:"
	@cat Makefile

isset-%:
	@if [ -z '${${*}}' ]; then echo 'ERROR: variable$ * not set' && exit 1; fi

dev:
	python -m pip install --upgrade pip
	pip install -e .[dev]

lint:
	pylint --rcfile=.pylintrc . || true

test: dev
	pytest tests/ -vv --cov=. --cov-report=html --cov-report=term-missing --junitxml=junit/coverage-results.xml

build: clean
	python -m pip install --upgrade pip
	pip install wheel
	python setup.py bdist_wheel

clean:
	@rm -rf .pytest_cache/ .mypy_cache/ junit/ build/ dist/
	@find . -not -path './.venv*' -path '*/__pycache__*' -delete
	@find . -not -path './.venv*' -path '*/*.egg-info*' -delete

install-package-databricks: isset-PACKAGE_NAME
	@echo Installing 'dist/${FULLNAME}' on databricks...
	databricks fs cp dist/${PACKAGE_NAME} dbfs:/libraries/${PACKAGE_NAME} --overwrite
	databricks libraries install --whl dbfs:/libraries/${PACKAGE_NAME} --cluster-id ${CLUSTER_ID}
