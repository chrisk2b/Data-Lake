# Purpose of the Repository

This repository contains the results of the "Data Lake" Project which is part of the [Udacity](https://www.udacity.com/) Data Engineering Nanodegree. Its’s purpose is to give the reviewers access to the code. 

# Summary of the Project
The project deals with a start up called Sparkify which is an online music provider. Sparkify wants to collect and analyze data about its user activities. It is e.g. interested in a detailed understanding of which songs users are currently listening. 

Sparkify is currently using a data warehouse to analyze its user data but it plans move the data warehouse to a data lake. The data of Sparkify resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project, we will build an ETL pipeline that extracts raw data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. The Spark process will be deployed on a cluster using Amazon EMR. 

The dimensional tables can be used by Sparkify's analytics team to continue finding insights in what songs their users are listening to.

# Description of the ETL Pipeline
As mentioned above, the raw data is located in S3. The ETL process is implemented using Spark. Therefore, a running Spark cluster on AWS is required. A Spark cluster can be constructed by using just EC2 machines or by using a managed version, namely Amazon EMR. In order to programmatically access S3 using Spark, a public-private key pair is necessary which can be generated using an Amazon account. The ETL pipeline uses Sparks Data Frame API: the raw data is extracted from S3 and stored in a Spark Data Frame. By doing that, a Schema gets ingested to the data. This is knows as 'Schema on Read' (as opposed to 'Schema on Write'): the data is not inserted to a database but the Data Frame behaves like a table in a database. Afterwards, Sparks SQL API for Data Frames is used to transform the data to its target form, namely a dimension data model consisting of a fact table and several dimensions. Afterwards, the transformed data is loaded back to S3 as Parquet files.

# Raw Datasets
We will be working with two datasets that reside in S3. Here are the S3 links for each:

-   Song data:  `s3://udacity-dend/song_data`
-   Log data:  `s3://udacity-dend/log_data`

These datasets are visible to everyone. The song data contains data about all available songs at Sparkify while the log data contains data about user activity, i.e. "which user is listening to which song at which time".

# Files in the Repository
The following files are contained in the repository:

 - [etl.py](https://github.com/chrisk2b/Data-Lake/blob/master/etl.py): This file contains the code which implements the ETL process and consists mainly of SQL-statements  are applied to Spark Data Frames and transform the extracted raw data into its target form. Further,  Sparks read and write methods are used to read the raw data from S3 and write it back to S3.
 - [dl.cfg](https://github.com/chrisk2b/Data-Lake/blob/master/dl.cfg): This is a config file which contains the AWS public-private key pair for programmatic access. It is loaded in etl.py to define environment variables which contain the public and the privet key. This allows Spark to read/write data from/to Amazon S3.
 - [README.md](https://github.com/chrisk2b/Data-Lake/blob/master/README.md) This README.

# How to run the ETL pipeline
This section describes the steps which are necessary to run the ETL pipeline:
 1. As mentioned above, Spark must be able to read data from S3. Therefore, the public-private key pair in  [dl.cfg](https://github.com/chrisk2b/Data-Lake/blob/master/dl.cfg) must correspond to an IAM user which has read and write access for S3. 
 2. In the script [etl.py](https://github.com/chrisk2b/Data-Lake/blob/master/etl.py) an output path must be specified in line 217: `output_data =  <path to output location>`. An S3 Bucket under this location must created upfront.
 3. Run the command `python etl.py` in a terminal. This will execute the ETL-process which extracts the raw data from S3, transforms them into a dimensional model which gets saved on S3 as well.

# Summary of the Target Datamodel
The parquet files loaded back to S3 contain the data in terms of a star schema. Below is a description of its structure:

 - **Dimension Tables**:
	 1.  **users** - users of Sparkify
    -   _user_id, first_name, last_name, gender, level_
      2.  **songs** - songs in music database
    -   _song_id, title, artist_id, year, duration_
   3. **artists** - artists in music database
    -   _artist_id, name, location, latitude, longitude_
   5.  **time** - timestamps of records in **songplays** (cf. fact tables below) broken down into specific units
    -   _start_time, hour, day, week, month, year, weekday_
 - **Fact Tables**:
    1.  **songplays** - records in log data associated with song plays 

	-   _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

The parquet files can be used to quickly load the data in e.g. an in-memory database for analytics. Of course, they can also be used to query the data-lake directly by using e.g. Amazon Athena.


# Files in the
 

	 


 

The major part of the project is to design a data model which best suits the analytics needs. Further, an ETL pipeline, which loads and transforms the raw JSON data into the database must be developed. The underlaying database technology is Postgres. Files in the Repository
The repository contains the following files/folders:
# Summary of the Datamodel
The Datamodel is based on a Star Schema an consist of the following tables:

 - **Dimension Tables**:
	 1.  **users** - users of Sparkify
![users dimension table](https://github.com/chrisk2b/Datamodelling-Postgres/blob/master/images/users.PNG)
    -   _user_id, first_name, last_name, gender, level_
      2.  **songs** - songs in music database
    -   _song_id, title, artist_id, year, duration_
   3. **artists** - artists in music database
    -   _artist_id, name, location, latitude, longitude_
   5.  **time** - timestamps of records in **songplays** (cf. fact tables below) broken down into specific units
    -   _start_time, hour, day, week, month, year, weekday_
 - **Fact Tables**:
    1.  **songplays** - records in log data associated with song plays 

	-   _songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent_

# Files in the Repositoty
The following files/folders are contained in the reposi