# Project README

### This project is for demo purposes only. If used, please update creds with env variables.

## Overview

This project is a demonstration of an ETL (Extract, Transform, Load) pipeline using Apache Airflow, Apache Spark, and MinIO. The pipeline is designed to fetch, process, and store stock market data for a specific stock symbol, AAPL. The project showcases the integration of various technologies to automate data workflows and is structured to be modular and scalable.

## Project Structure

- **dags**: Contains the Airflow DAGs that define the workflow for the ETL process.
- **include**: Contains additional Python scripts used by the DAGs.
- **spark/notebooks**: Contains the Spark application for data transformation.
- **Dockerfile**: Defines the Docker image for running the Spark application.

## Workflow Description

The ETL pipeline is orchestrated by an Airflow DAG, which consists of several tasks:

1. **API Availability Check**:

   - A sensor task checks the availability of the stock API to ensure data can be fetched.

2. **Fetch Stock Prices**:

   - A Python task fetches the stock prices for AAPL from a specified API.

3. **Store Prices in MinIO**:

   - The fetched data is stored in a MinIO bucket as a JSON file.

4. **Format Prices with Spark**:

   - A DockerOperator runs a Spark application to transform the stored JSON data into a structured CSV format.

5. **End Task**:
   - Marks the completion of the DAG.

## Spark Application

The Spark application is responsible for transforming the raw JSON data into a structured CSV format. It performs the following operations:

- Reads the JSON data from MinIO.
- Explodes and zips arrays to flatten the data structure.
- Writes the transformed data back to MinIO as a CSV file.

## Docker Configuration

The Spark application runs inside a Docker container, which is configured to include necessary dependencies for accessing MinIO. The Dockerfile specifies the base image and additional JAR files required for Hadoop S3A support.

## Running the Project Locally

1. **Start Airflow**:

   - Use the command `astro dev start` to spin up the necessary Docker containers for Airflow components.

2. **Access Airflow UI**:

   - Navigate to `http://localhost:8080/` and log in with the default credentials (`admin`/`admin`).

3. **Trigger the DAG**:
   - In the Airflow UI, trigger the `stock_market` DAG to start the ETL process.

## Diagram Explanation

The diagram illustrates the ETL process flow:

- **Start**: The process begins with an empty operator indicating the start of the DAG.
- **API Availability Check**: A sensor task checks if the stock API is available.
- **Fetch Stock Prices**: A Python task retrieves stock prices for AAPL.
- **Store Prices in MinIO**: The data is stored in MinIO as a JSON file.
- **Format Prices with Spark**: A DockerOperator runs a Spark job to format the data into CSV.
- **End**: The process concludes with an empty operator indicating the end of the DAG.

## Conclusion

This project demonstrates a simple yet effective ETL pipeline using modern data engineering tools. It highlights the use of Airflow for orchestration, Spark for data processing, and MinIO for storage, providing a foundation for more complex data workflows.
