.PHONY: all lint typecheck

all: typecheck lint

typecheck:
	mypy .

lint:
	ruff check .
