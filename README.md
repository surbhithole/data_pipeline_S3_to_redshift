# data_pipeline_S3_to_redshift

A music streaming startup, 'Sparkify', currently has JSON logs for user activity, as well as JSON metadata for songs in the application. These JSON documents currently reside in S3.

The aim of the project is to build an ETL pipeline, which will extract the data from S3, stage the data in redshift, and subsequently transform the data into a set of dimension tables in redshift, which can then be used for analysis of application usage.

From an analytics perspective, the 'Sparkify' team wishes to be able to find insights into which songs their users are listening to.

### Schema selection

![alt text](https://github.com/surbhithole/data_modelling_using_postgres/blob/main/sparkify_erd.png)

### Steps of the ETL process

The ETL pipeline comprises of two steps;

   1) Loading the data into the staging tables on redshift from S3.
   2) Populating analytics tables in redshift from the staging tables.

In the first step, we wish to copy the data from two directories of JSON formatted documents, staging_events, and staging_songs. For the staging songs, we are provided with the format of the data in 's3://udacity-dend/log_json_path.json' and hence we COPY the staging events using this document as the format. With regards to staging songs, no format is provided, and hence we use the 'auto' format for copying across the data.

With regards to creating the analytics tables, we firstly create the songs table from the staging_songs table, using 'SELECT DISTINCT' statements to avoid duplicates in the songs. We do the same for artists, and users respectively, once again using a 'SELECT DISTINCT' statement to avoid duplication. To create the songplays analytics tables, select from the staging_events table, joining artists and songs tables to retrieve the song_ids and artists_ids. We filter the insert statement by the entries for which page is equal to 'NextSong'. We create the time table from the songplays. we use the 'extract' function, to extract the particular part of the datetime object, and the 'timestamp' function to convert the epoch timestamp to a datetime object.

The ETL pipeline is such that the script will load all events from the json files into staging tables, and subsequently into analytics tables, such that the sparkify analytics team can produce valuable insights into user listening behaviour.

### How to execute files

>> python create_tables.py

>> python etl.py

