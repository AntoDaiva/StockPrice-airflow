# StockPrice-airflow
Stock Price ETL airflow using Docker

## Overview

This repository contains an ETL pipeline for extracting, transforming, and loading stock price data. The pipeline is orchestrated using Apache Airflow, and Python is used for scripting the ETL processes. The goal is to provide a flexible and scalable solution for collecting and managing historical stock price information. And this Airflow is run on a Docker

## Features

- **Modular Design:** The pipeline is structured in a modular way, allowing easy extension and customization of each ETL step.

- **Airflow Orchestration:** Apache Airflow is used as the orchestration tool to schedule and monitor the execution of ETL tasks.

- **Data Extraction:** The pipeline supports data extraction from various sources, such as financial APIs and CSV files.

- **Data Transformation:** Python scripts handle the transformation of raw data into a format suitable for analysis and reporting.

- **Data Loading:** Processed data is loaded into a PostgreSQL database for storage and further analysis.

## Docker Setup

**Requirement:** installed docker

Follow these instructions to build and run the Docker container

1. **Open Root Directory**
2. **Build Docker Image:**
   ```bash
   docker-compose build
   ```
3. **Run Docker Container:**
   ```bash
   docker-compose up -d
   ```

