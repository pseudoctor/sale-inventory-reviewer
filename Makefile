PYTHON ?= ./venv/bin/python
PIP ?= ./venv/bin/pip

.PHONY: bootstrap sync doctor lint compile test check run

bootstrap:
	python3 -m venv venv
	$(PYTHON) -m pip install --upgrade pip
	$(PIP) install -r requirements.lock

sync:
	$(PIP) install -r requirements.lock

doctor:
	$(PYTHON) scripts/health_check.py

lint:
	$(PYTHON) -m ruff check .

compile:
	$(PYTHON) -m py_compile scripts/generate_inventory_risk_report.py scripts/core/*.py

test:
	$(PYTHON) -m pytest -q

check: doctor lint compile test

run:
	./run.sh
