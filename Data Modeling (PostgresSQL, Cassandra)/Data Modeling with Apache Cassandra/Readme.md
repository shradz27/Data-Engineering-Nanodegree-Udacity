# **Udacity Data Engineer Project - Data Modeling with Cassandra**

A startup called Sparkify wants to analyze the data its been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, there isn't an easy way to query the data to generate the results, since the data resides in a directory of CSV files on user activity on the app.

We are creating an Apache Cassandra database for analysis which can create queries on song play data to answer questions. 


### ETL and data modeling

The dataset is called event_data which is in a directory of CSV files partitioned by data. The filepaths are given as:
event_data/<yyyy>-<mm>-<dd>-events.csv where <yyyy> indicates the year, <mm> indicates the month and <dd> indicates the year.

The ETL pipeline and data modeling are together in the jupyter notebook, **Project_1B_ Project_Template.ipynb**.
    
ETL copies data from the date-partitioned csv files to a single csv file **event_datafile_new.csv** which is then used to populate the denormalized Cassandra tables optimised for the 3 queries below. The 3 tables in the model are named after the song play query they are created to solve:

1. **song_history_by_session** queries artist, song title and song length information for a given `sessionId` and `itemInSessionId`.

2. **song_history_by_user** queries artist, song and user for a given `userId` and `sessionId`.

3. **song_history_by_song** queries user names for a given song.

    
## Files
    
1. **event_data** folder nested at the home of the project, where all needed data reside.
2. **Project_1B_ Project_Template.ipynb** the etl and 3 specific queries.
3. **event_datafile_new.csv** a smaller event data csv file that will be used to insert data into the Apache Cassandra tables.
4. **images** a screenshot of what the denormalized data should appear like in the event_datafile_new.csv. 
5. **README.md** current file, provides discussion on my project.
    
## Use

Launch Project_1B_ Project_Template.ipynb and run each component.