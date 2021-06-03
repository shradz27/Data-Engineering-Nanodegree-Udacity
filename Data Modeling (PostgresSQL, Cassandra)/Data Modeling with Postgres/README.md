### Introduction

A startup called Sparkify wants to analyze the data its been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, Sparkify does not have an easy way to query its data, which resides in a directory of JSON logs of user activity on the app, as well as a directory with JSON metadata on the songs in their app.

They want a data engineer to create a Postgres database with tables designed to optimize queries on song play analysis

### Database schema design and ETL pipeline

Built a star schema database using the following fact and dimension tables:

#### Fact Table

1. `songplays` - records in log data associated with song plays i.e. records with page NextSong

        - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### Dimension Tables

1. `users` - users in the app

         - user_id, first_name, last_name, gender, level

2. `songs` - songs in the music database

         - song_id, title, artist_id, year, duration

3. `artists` - artists in the music database

         -  artist_id, name, location, latitude, longitude

4. `time` - timestamps of records in songplays broken down into the specific unit

         - start_time, hour, day, week, month, year, weekday

Built ETL pipeline using python 

### Files

1. [`create_tables.py`](create_tables.py) drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
2. [`etl.ipynb`](etl.ipynb) reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
3. [`etl.py`](etl.py) reads and processes files from song_data and log_data and loads them into your tables.
4. [`sql_queries.py`](sql_queries.py) contains all the SQL queries, and is imported into the last three files above.
5. [`test.ipynb`](test.ipynb) displays the first few rows of each table to check the database.

### Use

Run [`create_tables.py`](create_tables.py) before running [`etl.py`](etl.py) to reset the tables

        create_tables.py has to run before any other files to create the sparkify database

Run [`test.ipynb`](test.ipynb) to test that the records were inserted successfully