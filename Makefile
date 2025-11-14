format: lint
	uv run -- ruff format

lint:
	uv run -- ruff check --fix

test:
	uv run -- pytest -v -n auto

upgrade:
	uv sync --upgrade --all-extras

reset:
	cp -R src/tests/test_archive/* src/tests/test_directory/
	rm -rf src/tests/test_duplicate_files/*