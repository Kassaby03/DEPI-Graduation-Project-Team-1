# DEPI Graduation Project Team 1: Weather Data Warehouse ETL Pipeline

This project implements a robust Extract, Transform, Load (ETL) pipeline designed to ingest real-time weather observation data from an operational database into a dimensional data warehouse for analytical reporting. The solution utilizes Apache Airflow for reliable workflow orchestration and Python for complex ETL logic.

-----

## Project Purpose

The primary goal of this initiative is to transition high-volume, transactional weather data into a well-structured **Star Schema** data warehouse. This separation allows for efficient historical analysis, enables faster query performance for business intelligence tools, and supports the evaluation of various weather forecast data models.

-----

## Technical Stack and Components

The architecture relies on industry-standard tools for data engineering and management:

  * **Orchestration:** Apache Airflow
  * **Databases:** SQL Server (used for both the **Operational Database - WeatherDB** and the **Dimensional Data Warehouse - WeatherDW**)
  * **ETL Scripting:** Python with the `pyodbc` library for secure database connectivity.
  * **Containerization:** Docker/Docker Compose for defining and running the multi-service environment.

The key files in this repository define the system:

  * `weather_to_dwh_pipeline.py`: Contains the Airflow DAG definition and the complete Python ETL function.
  * `WeatherDB.sql`: Defines the schema for the normalized source database.
  * `WeatherDW.sql`: Defines the dimensional (Star Schema) tables and relationships for the data warehouse.

-----

## Deployment and Execution

### Environment Setup

The solution is designed to run within a containerized environment. Prior to execution, Docker and Docker Compose must be installed. The SQL Server instance must be accessible to the Airflow container, typically via `host.docker.internal:1433`, using the specified credentials (`docker_user`, `PWD=9512`).

### Initialization Steps

1.  **Database Creation:** Run the SQL scripts (`WeatherDB.sql` and `WeatherDW.sql`) to create the necessary source and target database schemas.

2.  **Airflow Startup:** Initialize the Airflow database and start the services.

    ```bash
    docker compose run airflow-webserver airflow db init
    docker compose up -d
    ```

### Pipeline Operation

The ETL process is orchestrated by the **`weather_to_dwh_pipeline`** DAG. It is scheduled to run **hourly** (`0 * * * *`), ensuring the data warehouse is consistently updated with the latest weather observations.

-----

## ETL Process Details

The core logic, contained within the `load_to_warehouse` Python function, handles the Extract, Transform, and Load steps:

1.  **Extract:** Data is queried from the `WeatherDB` by joining `WeatherObservation`, `City`, and `WeatherCondition` tables to gather a complete set of attributes.
2.  **Transform and Dimension Loading:** Each extracted record is processed to ensure dimension integrity using a **Type 1 Slowly Changing Dimension (SCD)** approach:
      * It checks for the existence of records in **DimLocation**, **DimWeatherEvent**, **DimTime**, and **DimForecastModel**.
      * New, unique dimension records are inserted.
      * Necessary attributes like **Season** are derived from the observation timestamp.
      * The respective **surrogate keys** are retrieved for subsequent fact loading.
3.  **Fact Loading:** A new row is inserted into the **FactWeatherObservation** table. This row links all retrieved dimension keys (`time_id`, `location_id`, `event_id`, `model_id`) with the quantitative weather metrics (temperature, humidity, pressure, etc.).

-----

## Project Team

This project was developed collaboratively by the following members:

  * Abdelrahman Tarek
  * Omar Ahmed
  * Hussein Yahia
  * Rawan Desouky
  * Maryam Ahmed
  * Asmaa Khaled

-----

Do you require a detailed explanation of a specific transformation rule or a guide on querying the resulting `WeatherDW` schema?
