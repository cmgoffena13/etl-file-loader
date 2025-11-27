format: lint
	uv run -- ruff format

lint:
	uv run -- ruff check --fix

test:
	uv run -- pytest -v -n auto

upgrade:
	uv sync --upgrade --all-extras

install:
	uv sync --frozen --compile-bytecode
	
down:
	docker compose down

dev: dev-postgres

dev-postgres: reset
	docker compose up -d postgres
	sleep 1
	ENV_STATE=dev DEV_DATABASE_URL=postgresql+psycopg://fileloader:fileloader@localhost:5432/fileloader uv run python main.py

dev-mysql: reset
	docker compose up -d mysql
	sleep 1
	ENV_STATE=dev DEV_DATABASE_URL=mysql+pymysql://fileloader:fileloader@localhost:3306/fileloader uv run python main.py

dev-sqlserver: reset
	docker compose up -d sqlserver sqlserver-init
	sleep 5
	ENV_STATE=dev DEV_DATABASE_URL='mssql+pyodbc://sa:FileLoader123!@localhost:1433/fileloader?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes' DEV_SQL_SERVER_SQLBULKCOPY_FLAG=false uv run python main.py

dev-sqlserver-bulk: reset
	docker compose up -d sqlserver sqlserver-init
	sleep 5
	docker compose up file-loader-sqlserver-bulk

dev-bigquery: reset
	ENV_STATE=dev DEV_DATABASE_URL=bigquery://crypto-topic-479022-e7/test uv run python main.py

reset:
	cp -R src/tests/test_archive/* src/tests/test_directory/
	rm -rf src/tests/test_duplicate_files/*

profile-sqlserver: reset
	docker compose up -d sqlserver sqlserver-init
	sleep 5
	ENV_STATE=dev DEV_DATABASE_URL='mssql+pyodbc://sa:FileLoader123!@localhost:1433/fileloader?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes' DEV_SQL_SERVER_SQLBULKCOPY_FLAG=true uv run scalene main.py
