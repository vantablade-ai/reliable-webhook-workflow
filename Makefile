.PHONY: install test check demo run worker recover
install:
	python -m pip install -r requirements-dev.txt
test:
	pytest -q
check:
	ruff check .
	python -m compileall -q app tests scripts
	pytest -q
demo:
	python scripts/demo.py
run:
	uvicorn app.main:app --reload
worker:
	python scripts/worker_once.py
recover:
	python scripts/recover_once.py
