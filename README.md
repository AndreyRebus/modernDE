# modernDE

This repository provides a small environment for experimenting with modern data engineering tools. The stack centers around the Trino query engine with Iceberg tables stored in S3 (Yandex Cloud) and PostgreSQL used as the catalog backend.

## Features

- **Docker Compose** configuration that launches Trino with password authentication and a PostgreSQL service.
- **Initialization script** `init.sql` creates the necessary Iceberg metadata tables and an example schema for match data.
- **Trino configuration** in `etc/` with TLS support and an Iceberg catalog that points to an S3 bucket.
- **Helper scripts** for installing Docker and generating Trino configuration (`install-docker.sh`, `docker_install_rasp_pi.sh`, `trino_config_setup.sh`).
- **Python loader** `load.py` fetches League of Legends matches via the Riot API, stores them as Parquet files in S3 and registers the files in the Iceberg table through Trino.
- **Environment template** `.env.example` lists variables for database credentials, API keys and S3 access.

## Quick start

1. Copy `.env.example` to `.env` and fill in your secrets and connection details.
2. Run `docker compose up -d` to start Trino and PostgreSQL. The SQL in `init.sql` will be executed automatically during container startup.
3. Connect to Trino on `https://localhost:8443` using the credentials from `etc/password.db`.
4. Execute `python load.py` to download match data and populate the Iceberg table.

## Directory overview

- `docker-compose.yml` – container definitions for Trino and PostgreSQL.
- `etc/` – configuration files mounted into the Trino container.
- `init.sql` – creates metadata tables and the example Iceberg table.
- `load.py` – script that loads match data from the Riot API into S3 and registers it with Iceberg.
- `install-docker.sh`, `docker_install_rasp_pi.sh` – scripts to install Docker on different platforms.
- `trino_config_setup.sh` – interactive helper to generate Trino keystore and password files.

This setup can be used as a starting point for testing Trino with Iceberg and experimenting with data loading pipelines.
