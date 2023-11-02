sources := src tests

build:
	hatch build

install:
	pip install --upgrade pip
	pip install -r dev-requirements.txt
	pip install -e .

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
