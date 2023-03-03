# Video Games Sales Analysis

Video Games Sales Analysis is a exersise where we need to create a simple process where we need to process data similar to the ETL process in Data Warehousing. The goals of the exersise are:

* Create database model based on initial data analysis
* Create process to ingest data into staging table
* Create process to load data into facts and dimensions
* Write couple of SQL statements to showcase some interesting results

### Technologies

Even though the same results can be achieved using various tools and technologies, this analysis is using only two of them:

* Postgres Database
  * Locally inside a Docker Container
* Python 3.9.11
  * Libraries can be found in requirements.txt file
  * .py scripts and .ipynb notebooks

### Helper Scripts

In the project, there are scripts that are not a part of the ETL process itself, but are there to help analyze data or to create objects in Postgres. Such files are marked with a number as a prefix (e.g. 00-).

### Data Model

The model is a version of a Snowflake schema - multi-dimensional data model that is an extension of a star schema, where dimension tables are broken down into subdimensions. The changes in dimensions are not expected, therefore in this case we will only update existing record without storing the old version.

On the other hand, facts will store historical info. When a new version of a row gets ingested into the fact table, the old record will be assign with "record_end_timestamp" value. That record is no longer representing current state, but is still expected to be used in historical analysis.

### ETL

The ETL process was created using Python. There are five steps in the process and if any of the steps fails the script gets terminated. The steps are:

1. Extract from flat file (source)
2. Prepare data before ingesting it to staging
3. Ingest data to staging
4. Execute stored procedures used to ingest data from staging to dimension and fact tables
5. Truncate staging table

File responsible is main_etl.py

### DB Dump

Postgres database can be found in video_games_db.sql file and can easily be used to "restore" the database will all of the objects and data.

### Results of SQL Statements

To showcase some interesting results, you can open **sql_analysis.ipynb** notebook where you can see the output of the queries, but also some simple diagrams making results more effective and presentable.
