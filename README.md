# FileLoader

FileLoader is a robust Data Engineering service that ingests files into a database.

 - Mutliple Ingestion File Formats Supported (CSV, Excel, JSON, Parquet)
    - Supports Gzip Compression
 - Multiple File Systems Supported (Local/Shared Drive, AWS S3, Azure Blob Container, GCP Bucket)
    - Use your preferred File System
 - Multiple Target Databases Supported (MySQL, Postgres, SQL Server, BigQuery)
     - Use your preferred Database
 - OpenTelemetry Logging
    - Use your preferred logging platform
 - SMTP Protocol for Email Alerts to Business Stakeholders
    - Use your preferred SMTP service
 - Webhook Integration for Internal Alerts
    - Use your preferred communication platform
 - Dockerized Application to deploy anywhere

## Why Files?
Files are essential for non-technical roles at companies because they allow anyone to organize, analyze, and share data without requiring programming skills. Files are:
 - Accessible
 - Portable
 - Structured
 - Standardized
 - Decoupled From Software Systems

## Why Databases?
Databases are essential for technical roles at companies because they allow for scalable, reliable, and maintainable data management. Databases are:
 - Accessible
 - Secure
 - Structured
 - Standardized
 - Decoupled From Software Systems

Ironic, yet the key difference is databases are far more reliable and scalable than files. The problem is they require technical skills to interact with, such as knowing SQL. The even bigger problem is bridging the gap between these two, when technical knowledge is limited and a company needs to move forward. Cue FileLoader.

## Table of Contents

- [Features](#features)
  - [Scalability](#scalability)
  - [Reliability](#reliability)
  - [Maintainability](#maintainability)
- [Design](#design)
  - [FileHelper](#filehelper)
  - [Processor](#processor)
  - [PipelineRunner](#pipelinerunner)
  - [Reader](#reader)
  - [Validator](#validator)
  - [Writer](#writer)
  - [Auditor](#auditor)
  - [Publisher](#publisher)
- [Production Setup](#production-setup)
  - [File Helper Platform](#file-helper-platform)
    - [Local/Shared Drive](#localshared-drive)
    - [AWS S3](#aws-s3)
    - [Azure Blob Container](#azure-blob-container)
    - [GCP Bucket](#gcp-bucket)
  - [OpenTelemetry Logging](#opentelemetry-logging)
  - [Target Database](#target-database)
    - [MySQL](#mysql)
    - [Postgres](#postgres)
    - [SQL Server](#sql-server)
    - [BigQuery](#bigquery)
  - [SMTP Emailing](#smtp-emailing)
  - [Webhook Alerting](#webhook-alerting)
  - [Source Configuration](#source-configuration)
  - [Entrypoint](#entrypoint)
  - [Testing](#testing)
- [Contributing](#contributing)
- [Benchmarks](#benchmarks)

## Features
### Scalability
 - Configuration Driven Pipelines
 - Parallel Processing of Files
 - Batch Processing for Memory Efficiency and Database Throttling
 - Dynamic Table Creation
 - Proper Table Indexing for Scalable Database Operations
 - Automatic Dead Letter Queue Cleanup
   - Upon Successful Reprocessing
### Reliability
 - Retries File & Database Operations to handle transient errors
 - Automatic File Grain Validation
    - Ensures the File is ready for Database integration
 - Validate Row-Level Data Via Pydantic Models
    - Validation Errors get sent to Dead Letter Queue table (default: any errors = failure)
 - Configurable Audit Queries
    - Audit File Data Holistically
### Maintainability
 - Automatic Email Notifications for File Failures
    - Send to Business Stakeholders for Accountibility
 - Detailed Dead Letter Queue
    - Empower Business Stakeholders to Resolve Row Level Issues
 - Type-Safe Repo Configuration Settings
 - Comprehensive Test Suite
 - Automatic Webhook Notifications for Internal Errors
 - Factory Pattern allows for easy implementation for new support
 - Detailed Pipeline Logging Stored for Performance Tracking

## Design

![FileLoader Design](static/FileLoaderDesign.png)

### FileHelper
The FileHelper class scans the file directory and creates a static queue of the file names. This queue is passed to the Processor. The FileHelper is also called to interact with files (archive/delete/move) during the pipeline run.

### Processor
The Processor class coordinates the work and assigns thread workers (PipelineRunners) to process the files in the static queue. Before work is assigned, the file names are compared against the source file configurations file patterns to determine which configuration should be used. Once a source configuration is matched, a PipelineRunner is assigned. Once all work is done, it checks the results for any internal errors that need to be surfaced to the data team.

### PipelineRunner
The PipelineRunner class is the data pipeline that the file passes through. It coordinates all of the pipeline classes while handling any errors gracefully. The PipelineRunner utilizes the FileHelper to archive the file to an archive directory before running; A dynamic stage table is also created using the source configuration. Before the PipelineRunner triggers the Reader class, a check is ran to see if the file has already been processed. If it fails the check, an email alert is triggered and the pipeline stops. If the file hasn't been processed, it calls the Reader class.   

Upon completion, the file is deleted out of the file drop directory and the stage table dropped to ensure a clean environment.

### Reader
The Reader class checks if the file has a header, if data is available, and if the file contains all of the columns required (as designated in the configuration). If any of the checks fail, an email alert is triggered and the pipeline stops. The Reader streams data from the file and yields a batch of data to the Validator class once the batch limit is reached.

### Validator
The Validator class utilizes the source configuration to validate each record of the batch it receives against a pydantic model. If a record fails validation, it is flagged and enriched with information for the Dead Letter Queue (validation failure reason(s), file row number, column(s) that failed, etc.). The Validator yields a batch of data to the Writer class once the batch limit is reached.  

The Validator also stores the number of records that failed validation overall and at the end compares against the declared validation threshold within the source configuration (default is zero validation error). If it is over the threshold, an email alert is triggered and the pipeline stops.

### Writer
The Writer class divides the batch it receives into two categories: validated records that go into a staging table and records that failed validation that go into the Dead Letter Queue table. When a category reaches the batch limit, the category is inserted into its associated table. This continues until the Writer stops receiving batches from the Validator. Once the writes have been complete, responsibility is passed to the Auditor class.

### Auditor
The Auditor class checks if the entire file's data, stored in the staging table, complies with the expected grain found in the source configuration. It then executes any custom aggregate audit queries given within the source configuration. If either of these checks fail, an email alert is triggered and the pipeline stops. If all audit checks pass, responsibility to passed to the Publisher class.

### Publisher
The Publisher class merges the staging table data into the target table (the final destination). It handles inserts/updates appropriately to sync the target table with the new data provided in the staging table.

## Production Setup

### File Helper Platform

First, you need to decide the locations for the File Directory, Archive Directory, and Duplicate File Directory. Current support:  
 - Local/Shared Drive
 - AWS S3
 - Azure Blob Container
 - GCP Bucket

Regardless of your choice, you will need to declare the three environment variables and *create the directories*:
 - `PROD_DIRECTORY_PATH`
 - `PROD_ARCHIVE_PATH`
 - `PROD_DUPLICATE_FILES_PATH`

#### Local/Shared Drive
You will need to ensure the FileLoader service has access.
Example Variables:
```
PROD_DIRECTORY_PATH=src/tests/test_directory
PROD_ARCHIVE_PATH=src/tests/test_archive
PROD_DUPLICATE_FILES_PATH=src/tests/test_duplicate_files
```

#### AWS S3
You have two options for authentication, AWS Session Token or Access ID/Secret Access pair. Example Variables:
```
PROD_FILE_HELPER_PLATFORM=aws
PROD_AWS_ACCESS_KEY_ID=A;SLDKJF;LKJASJDFA
PROD_AWS_SECRET_ACCESS_KEY_ID=sdfasdfas
PROD_DIRECTORY_PATH=s3://fileloader-test-asdf/test_directory/
PROD_ARCHIVE_PATH=s3://fileloader-test-asdf/test_archive/
PROD_DUPLICATE_FILES_PATH=s3://fileloader-test-asdf/test_duplicate_files/
```

#### Azure Blob Container
You have two options for authentication, the azure storage connection string or the storage account url and key pair. Example Variables:
```
PROD_FILE_HELPER_PLATFORM=azure
PROD_AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=adlsdevus;AccountKey=asdfasdf;EndpointSuffix=core.windows.net"
PROD_DIRECTORY_PATH=https://adlsdevus.blob.core.windows.net/filedrop/test_directory
PROD_ARCHIVE_PATH=https://adlsdevus.blob.core.windows.net/filedrop/test_archive
PROD_DUPLICATE_FILES_PATH=https://adlsdevus.blob.core.windows.net/filedrop/test_duplicate_files
```

#### GCP Bucket
Only option for authentication is google application credentials or handled in IAM. Example variables:
```
PROD_FILE_HELPER_PLATFORM=gcp
PROD_GOOGLE_APPLICATION_CREDENTIALS=name_of_file.json
PROD_DIRECTORY_PATH=gs://fileloader-test/test_directory/
PROD_ARCHIVE_PATH=gs://fileloader-test/test_archive/
PROD_DUPLICATE_FILES_PATH=gs://fileloader-test/test_duplicate_files/
```

### OpenTelemetry Logging
The FileLoader service uses OpenTelemetry for logs, you'll need to figure out the log and trace endpoints for your logging platform. Example variables:
```
PROD_OPEN_TELEMETRY_LOG_ENDPOINT=https://logfire-us.pydantic.dev/v1/logs
PROD_OPEN_TELEMETRY_TRACE_ENDPOINT=https://logfire-us.pydantic.dev/v1/traces
PROD_OPEN_TELEMETRY_AUTHORIZATION_TOKEN=asdfdsaf
```

### Target Database
You will need to pick the database you want to ingest the files into; this is also the database that will store the Dead Letter Queue table: `file_load_dlq` and the pipeline logging table: `file_load_log`. FileLoader utilizes SQLAlchemy for all database connections. Current support:
 - MySQL
 - Postgres
 - SQL Server
 - BigQuery

#### MySQL
Example Variable:  
`PROD_DATABASE_URL=mysql+pymysql://username:password@localhost:3306/database_name`

#### Postgres
Exampe Variable:
`PROD_DATABASE_URL=postgresql+psycopg://username:password@localhost:5432/database_name`

#### SQL Server
Python has insert limitations when it comes to SQL Server. I would highly recommend utilizing the `PROD_SQL_SERVER_SQLBULKCOPY_FLAG` flag to utilize `pythonnet`. You'll need the .NET runtime available. You can utilize the `Dockerfile.sqlserver` to spin up a container, otherwise you'll need to make sure that a .NET runtime is available on the running machine. Example variables:
```
PROD_SQL_SERVER_SQLBULKCOPY_FLAG=True
PROD_DATABASE_URL=mssql+pyodbc://username:password@localhost:1433/database_name?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes
```

#### BigQuery
Example Variable  
`bigquery://project_id/dataset_name`

### SMTP Emailing
FileLoader utilizes SMTP to send email alerts for file issues if the `notification_emails` config is set for a source. Email alerts are file specific and provide relevant information to help business stakeholders address file problems. Example variables:
```
# AWS SES Service
PROD_SMTP_HOST=email-smtp.us-east-2.amazonaws.com
PROD_SMTP_PORT=587
PROD_SMTP_USER=AWS_ACCESS_KEY_ID
PROD_SMTP_PASSWORD=AWS_SECRET_ACCESS_KEY
# Sender Email
PROD_FROM_EMAIL=DataTeamSender@hotmail.com
# Email that is always CCed for visibility
PROD_DATA_TEAM_EMAIL=DataTeam@hotmail.com
```

### Webhook Alerting
FileLoader supports webhook alerting for internal errors that are not file issues. Webhook alerting contains system information. This separates responsibilities between software and business stakeholders/external partners. Example variable:
`PROD_WEBHOOK_URL=https://hooks.slack.com/services/asdf/hjkl/ajsljdfjksj`

### Source Configuration
To use the service, you need to create source configurations for each file you expect to be dropped in the directory_path. You can see examples in `src/sources/systems/`

Each source configuration has a table model that is a pydantic model which determines the table structure for the tables that will be created (staging and target). The model also acts as a validation model for the records themselves. 

You can organize your source configs within `src/sources/systems` any way you like. After creating a source configuration, you'll want to import it in `src/sources/master.py` and make sure to add it to the `MASTER_REGISTRY`. That's it!

### Testing
The FileLoader repo has default directories and database containers that can be tested against. You can check out sample source configuration in `src/sources/systems/`. This provides an easy way to develop locally with test files and test the service before deployment.

### Entrypoint
There are a couple ways to utilize FileLoader. The most basic way is to run `main.py` in the repo and it will point towards the directory_path provided with your environment variables and process all the files that match your source configs. You can automate this trigger on a schedule.

FileLoader also has a CLI. This is convenient when you may need to process one specific file OR if you have multiple file drop directories and you want to inject the paths. I created a `fileloader` bash script shortcut within the main repo directory. Simply utilize the below command within the repo to trigger CLI commands:  

`./fileloader --help`

## Contributing
See the [contributing](CONTRIBUTING.md) doc on detailed approaches to add to the repo in regards to development. This could be implementing more file formats, another notifier, another database integration, etc.

## Benchmarks
I tested a local, 350MB (uncompressed), 2 million row parquet file with indepth validation utilizing a 100k batch size. The original file was obtained [here](https://github.com/datablist/sample-csv-files) and you can check the validation with the source configuration at `src/sources/systems/customer/customer.py`. Below are the benchmarks for each database.
 - Postgres: ~124 seconds
 - MySQL: ~430 seconds 
   - Problem with the publish, let me know what I'm doing wrong there in `src/pipeline/publish/mysql.py`. Could easily be as fast as Postgres probably. I'm not a MySQL guy.
 - SQL Server (SqlBulkCopy): ~154 seconds
 - SQL Server (No SqlBulkCopy): ~2657 seconds 
   - SQL Server Python limitations forced batch size to be 160 records. Kill me now.
 - BigQuery: ~320 seconds 
   - Probably faster with a bigger batch size. BigQuery handles big data better. Just have to respect memory constraints of FileLoader