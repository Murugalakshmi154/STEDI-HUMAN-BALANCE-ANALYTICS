STEDI Human Balance Analytics Project

Project Overview
	This project builds a cloud-based data lake and analytics pipeline for the STEDI Human Balance Analytics system using AWS S3, AWS Glue, and Amazon Athena. The pipeline ingests raw JSON sensor data, performs data cleaning, filtering, and joins, and produces curated datasets that are ready for machine learning model training and analytics.

Architecture
	The Data is flown through 3 Zones
	- Landing Zone (Raw JSON data stored in S3)
	- Trusted Zone (Filtered and cleaned data by AWS Glue)
	- Curated Zone (Joined Datasets used for machine learning analytics)

AWS Services Used
	- Amazon S3 (Data Storage)
	- AWS Glue (ETL processing)
	- Amazon Athena (SQL queries and validations)

Output
	- Trusted and Curated Datasets stored in S3
	- Athena Query Screenshots validating expected record counts
	- Python and SQL scripts for each data pipeline stage

