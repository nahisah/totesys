PYTHON_INTERPRETER=python
PIP=pip
SHELL=/bin/bash
PYTHONPATH=$(shell pwd)

create-environment:
	echo "Creating the venv..."
	$(PYTHON_INTERPRETER) -m venv venv
	echo "venv created"

install-requirements:
	echo "installing requirements..."
	source venv/bin/activate && $(PIP) install -r ./requirements.txt
	echo "requirements installed"

install-dev-tools:
	echo "installing dev tools..."
	source venv/bin/activate && $(PIP) install bandit black safety pip-audit pytest pytest-cov flake8 isort

security-check:
	source venv/bin/activate && bandit -lll */*.py *c/*.py && pip-audit

unit-test-initial:
	source venv/bin/activate && PYTHONPATH=$(PYTHONPATH) pytest @test/initial_tests.txt --cov

unit-test-load:
	source venv/bin/activate && PYTHONPATH=$(PYTHONPATH) pytest @test/load_tests.txt --cov

format-code:
	source venv/bin/activate && isort test/ src/ --profile black && black test/ src/

lint-code:
	source venv/bin/activate && flake8 test/ src/ --max-line-length=88 --ignore=E203,W503,E501

run-setup: create-environment install-requirements install-dev-tools security-check format-code lint-code
