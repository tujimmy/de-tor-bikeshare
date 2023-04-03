# Toronto Bike Share Data Engineering Zoomcamp Project 2023

## Overview
This data engineering project is part of the [Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp) and focuses on analyzing Toronto Bike Share data, specifically station availability. The objective is to apply the knowledge gained during the course to build a comprehensive data pipeline.

## Problem Statement
The main goal is to understand bike availability patterns and identify areas for improvement. Furthermore, the project will assess the impact of upcoming pricing changes on bike availability, scheduled for April 3rd, 2023.

For more details on the pricing changes, please visit [Recommended Pricing Changes 2023](https://bikesharetoronto.com/news/recommended-pricing-changes-2023/).

**Questions to answer:**

1. What are the availabilities of e-bikes before and after the price changes?
2. Which stations are frequently full or empty?
3. At what times are stations full or empty?

## About the Dataset

The Toronto Bikeshare data has been taken from City of Toronto's Open Data as an API that shows real-time data on all the bike stations. Since this is real time there is no historical data to analyze, so I have limited data from when the data was first ingested as of March 22nd, 2023.

**Station Status:** This dataset provides real-time information about each bike station, such as the number of available bikes, e-bikes, and open docks. In the project, this data is ingested every 10 minutes to capture the latest availability information. It plays a crucial role in understanding bike usage patterns and identifying stations that frequently experience shortages or surpluses of bikes.

**Station Information:** This dataset contains static information about each bike station, including the station's name, location, and capacity. It also indicates whether a station is a charging station for e-bikes. This data is ingested once a day and combined with the Station Status data in the data pipeline to create a comprehensive view of bike availability across the network. The information helps in understanding the impact of station attributes on bike availability and identifying areas that may require additional resources or infrastructure improvements.

The `bike_availability` table is a derived dataset generated by the data pipeline, which combines and aggregates information from the Station Status and Station Information datasets. This table provides a comprehensive view of bike availability across the Toronto Bikeshare network, with data segmented by station, date, and time period.

The table contains various metrics, such as average number of bikes, e-bikes, and open docks available at each station, as well as the average availability ratio. These metrics help identify patterns related to bike usage and station performance, making it easier to pinpoint areas that may require additional resources or infrastructure improvements.

[**Toronto Bike Share Data Source**](https://open.toronto.ca/dataset/bike-share-toronto/)

## Data Pipeline

The data pipeline consists of three Prefect flows:

1. **Ingest Station Status:**
    - Queries the station status API
    - Flattens the JSON results and saves them as a Parquet file in Google Cloud Storage every 10 minutes
2. **Ingest Station Information:**
    - Queries the station information API
    - Flattens the JSON results and saves them as a Parquet file in GCS once a day
3. **Bikeshare Reporting Pipeline:**
    - Runs once a day and consists of multiple tasks:
        - Create tables in BigQuery if they do not exist
        - Start Dataproc Cluster
        - Read Parquet files in GCS and load them into staging tables for station_status and station_information
        - Read the staging tables and create a bike_availability table with aggregated data
        - Stop Dataproc Cluster

## Project Architecture

The project architecture consists of a set of interconnected components designed to handle various aspects of data ingestion, processing, storage, and analysis. The key components are as follows:

- Data Ingestion: The system retrieves real-time Station Status and daily Station Information data from the City of Toronto's Open Data API. Prefect is used to orchestrate the data ingestion process, ensuring that the data is collected at the appropriate intervals.
- Data Storage: The ingested data is stored in Google Cloud Storage (GCS) as Parquet files. This format allows for efficient storage and quick retrieval of the data when needed. GCS provides a scalable and reliable storage solution for the project.
- Data Processing: Google Cloud DataProc is utilized to process the data using Apache Spark. This powerful combination enables the system to handle large-scale data processing tasks, transforming and aggregating the raw data into the bike_availability table.
- Data Warehouse: Google BigQuery serves as the data warehouse for the project, storing the processed and aggregated data in the bike_availability table. BigQuery allows for efficient querying and analysis of the data, making it an ideal choice for this project.
- Data Visualization: Looker is used for data visualization, enabling users to explore the data and uncover insights about bike availability and usage patterns. By leveraging Looker's powerful visualization capabilities, stakeholders can easily understand the results of the data analysis and make informed decisions.
- Infrastructure as Code (IaC): Terraform is used to manage the infrastructure components of the project. This IaC approach ensures that the infrastructure is easily reproducible and can be managed in a version-controlled manner.

The project architecture is designed to provide a scalable, reliable, and efficient solution for ingesting, processing, storing, and analyzing Toronto Bikeshare data, ultimately enabling stakeholders to make data-driven decisions and improve the system's performance.

## Results

## Reproduction steps

### Prerequisites
- Python 3
- Google Cloud Account
- Google Cloud Service Account JSON credentials for (Compute, BigQuery, DataProc, Cloud Storage admin)
- Prefect Cloud
- Terraform
- Git

1. Clone this repository
2. # Refresh service-account's auth-token for this session
    gcloud auth application-default login
2. Terraform
   Change to terraform directory
     cd terraform
    
    Initialize terrform
    terraform init

    Preview the changes to be applied:
    terraform plan
    It will ask for your GCP Project ID and a name for your DataProc Cluster

    Apply the changes:
    terraform apply

3. Install the required Python packages:
    pip install -r requirements.txt
4. Configure git secrets for git actions
    secrets.GCP_SA_KEY
    secrets.GCP_PROJECT_ID
    GCS_BUCKET_NAME in .github/workflows/deploy-to-gcs.yaml
5. Update prefect/config/config.json
6. Prefect Cloud login
    prefect cloud login
6. Prefect Block Setup
    GCP Cred (zoom-gcs-creds)
    GCS Bucket (zoom-gcs)
    GitHub (github-block)
8. Setup Agent (Local or VM)
    Local
    prefect agent start --pool default-agent-pool
    VM
    pip install -r requirements
    prefect cloud login
    prefect agent start --pool default-agent-pool

7. Deploy the Prefect Pipelines
    prefect deployment build prefect/ingest_station_status.py:etl_api_to_gcs --name ingest_station_status --tag main -sb github/github-block --cron "1-59/10 * * * *" --timezone US/Eastern -a
    prefect deployment build prefect/ingest_station_infomation.py:etl_api_to_gcs --name ingest_station_infomation --tag main -sb github/github-block --cron "1 0 * * *" --timezone US/Eastern -a
    prefect deployment build prefect/bikeshare_reporting_pipeline.py:bikeshare_reporting_pipeline --name bikeshare_reporting_pipeline --tag main -sb github/github-block --cron "1 1 * * *" --timezone US/Eastern -a

