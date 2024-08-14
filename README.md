# Pinterest Data Pipeline Project
## Description 
This data project is part of the AiCore Data Engineering Bootcamp. It involves building a data pipeline similar to Pinterest. Pinterest processes billions of data points daily to enhance user experience. his project will demonstrate how to configure a data pipeline on AWS using various tools and services.

By the end of this project, I will have:
- Configured an EC2 instance to use an Apache Kafka Client
- Built an API to send data to an MSK cluster 
- Used Databricks to read and process data from AWS 
- Orchestrated Databricks workloads using AWS Managed Workflows for Apache Airflow (MWAA)
- Sent streaming data to Kinesis
- Used Sparks to perform data cleaning and computation


## Features 
1) AWS Services: EC2, MSK, S3, API Gateway, Kinesis, MWAA
2) Apache Kafka: Message brokering for data streams
3) Databricks: Data processing and analytics
4) Apache Spark: Distributed data processing
5) AWS MWAA (Managed Workflows for Apache Airflow): Workflow orchestration and automation of data pipelines

## Project Structure
A. Batch Processing 

1) Configure EC2 Kafka Client 

    Set up an EC2 instance as a Kafka client to interface with the MSK cluster.

2) Connect a MSK Cluster to an S3 bucket 

    Link the MSK cluster to an S3 bucket for batch data storage and retrieval.

3) Configure an API in API Gateway 

    Set up an API Gateway to facilitate data ingestion through a RESTful API.

4) Batch Processing: Sparks on Databricks 

    Use Apache Spark on Databricks to process and analyse batch data from S3.

5) AWS Managed Workflows for Apache Airflow (MWAA)

    Implement AWS Managed Workflows for Apache Airflow (MWAA) to orchestrate the batch data processing pipeline.

B. Stream Processing 
1) Create data streams using Kinesis Data Streams

    Set up Kinesis Data Streams to handle real-time data streaming.

2) Configure API 

    Create an API to receive data and forward it to Kinesis streams

3) Send data to Kinesis streams 

    Stream incoming data to Kinesis Data Streams for real-time processing.

4) Read data from Kinesis streams and transform in Databricks 

    Ingest and transform streaming data from Kinesis in Databricks using Spark.

5) Write streaming data to Delta tables 

    Persist the transformed streaming data into Delta Lake tables for downstream analytics.


## Prerequisites
- AWS Account: Required for using AWS services like EC2, S3, MSK, Kinesis, and MWAA
- Databricks Account: For data processing and analytics
- Python: Ensure Python is installed along with the necessary libraries

## Installation

To set up the environment for this project, you can install all the required Python packages listed in the requirements.txt file.
