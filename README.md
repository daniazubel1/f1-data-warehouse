# Formula 1 Data Warehouse

A modern Data Engineering project analyzing Formula 1 historical data.

## Architecture

- **Ingestion**: Apache Airflow pulling from Ergast API
- **Storage**: PostgreSQL (Staging + Warehouse)
- **Transformation**: dbt (Data Build Tool)
- **Visualization**: Metabase

## Getting Started

1.  **Prerequisites**: Docker & Docker Compose
2.  **Start Services**:
    ```bash
    docker-compose up -d
    ```
3.  **Access UI**:
    - Airflow: http://localhost:8080 (airflow/airflow)
    - Metabase: http://localhost:3000
