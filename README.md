# data_pipeline_S3_to_redshift

A music streaming startup, 'Sparkify', currently has JSON logs for user activity, as well as JSON metadata for songs in the application. These JSON documents currently reside in S3.

The aim of the project is to build an ETL pipeline, which will extract the data from S3, stage the data in redshift, and subsequently transform the data into a set of dimension tables in redshift, which can then be used for analysis of application usage.

From an analytics perspective, the 'Sparkify' team wishes to be able to find insights into which songs their users are listening to.

### Schema selection

![alt text](https://github.com/surbhithole/data_modelling_using_postgres/blob/main/sparkify_erd.png)

### Steps of the ETL process

1) Define the schema of the Database.
2) Create tables
3) Load data from S3 to Redshift
4) Insert data into the tables in redshift so that it can be used further by the analytics team for processing.

### How to execute files

>> python create_tables.py

>> python etl.py

