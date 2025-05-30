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
	source venv/bin/activate && $(PIP) install bandit black safety pip-audit pytest pytest-cov

security-check:
	source venv/bin/activate && bandit -lll */*.py *c/*.py && pip-audit

unit-test:
	source venv/bin/activate && PYTHONPATH=$(PYTHONPATH) pytest --cov

format-code:
	source venv/bin/activate && black test/ src/

run-checks: unit-test security-check

run-all: create-environment install-requirements install-dev-tools run-checks format-code
