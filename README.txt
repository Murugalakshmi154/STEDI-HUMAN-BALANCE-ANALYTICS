STEDI Human Balance Analytics Project

Project Overview
	This projects builds a data lakes and analytics pipeline for the STEDI human balance analytics using AWS S3, AWS Glue, Amazon Athena. The pipeline ingests raw JSON data, followed by filtering and joining datasets. It produces the curated datasets that are ready for Machine learning models training.

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
	- Athena Query Screenshots confirming the expected rows
	- Python and SQL scripts for each pipeline stage