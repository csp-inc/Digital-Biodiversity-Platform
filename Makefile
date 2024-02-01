SHELL=/bin/bash

clean:
	@find . -name '__pycache__' ! -path '*/node_modules/*' -exec rm -fr {} +
	@find . -name 'build' ! -path '*/node_modules/*' -exec rm -fr {} +
	@find . -name 'bin' ! -path '*/node_modules/*' -exec rm -fr {} +
	@find . -name 'obj' ! -path '*/node_modules/*' -exec rm -fr {} +
	@find . -name 'dist' ! -path '*/node_modules/*' -exec rm -fr {} +
	@find . -name 'publish' ! -path '*/node_modules/*' -exec rm -fr {} +

install-deps:
	@python3.8 -m pip install -r ./products/common/requirements.txt
	@python3.8 -m pip install -r ./products/biodiversity/requirements.txt

unit-test:
	# @PYTHONPATH=./products/common/src python3.8 -m pytest ./products/common/tests --doctest-modules
	@PYTHONPATH=./products/common/src:./products/biodiversity/src python3.8 -m pytest ./products/biodiversity/tests --doctest-modules
	@PYTHONPATH=./products/common/src:./products/biodiversity/src:./products/biodiversity/dataprep/src python3.8 -m pytest ./products/biodiversity/dataprep/tests --doctest-modules

lint:
	@echo "Running black formatting:"
	@python3.8 -m black --check ./products
	@echo "Running pyright linting & type checking:"
	@PYTHONPATH=./products/common/src:./products/biodiversity/src:./products/biodiversity/inference/src python3.8 -m pyright ./products
	@echo "Running flake8 linting:"
	@-python3.8 -m flake8 --statistics ./products

mk-lint:
	@markdownlint './**/*.md' --ignore ./node_modules
	@find . ! -regex '\(.*\/\.git.*\|.*\/node_modules.*\|.*\/.pytest_cache.*\|.*\/build.*\)' -name '*.md' -print0 | xargs -0 -n1 markdown-link-check -c .markdownlinkcheck.json

detect-secrets:
	@detect-secrets	scan

update-baseline:
	@detect-secrets scan --baseline .secrets.baseline

