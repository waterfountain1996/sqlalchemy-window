sources := src tests

test:
	coverage run

coverage:
	coverage report

typecheck:
	mypy $(sources)

lint:
	ruff check $(sources)

format:
	ruff --fix $(sources)
