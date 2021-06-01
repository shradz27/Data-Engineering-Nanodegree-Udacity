import configparser
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek, \
    monotonically_increasing_id
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Creates the Spark session

    PARAMS : None

    RETURNS: Spark
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Processes all the songs data from json files 

    PARAMS:
    spark : an active Spark session
    input_data : path to S3 bucket with input data
    output_data : path to S3 bucket to store output tables
    
    RETURNS: None
    """
    
    # get filepath to song data file
    song_data = "{}*/*/*/*.json".format(input_data)

    # read song data file
    df = spark.read.json(song_data).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select(col("song_id"), \
                            col("title"), \
                            col("artist_id"), \
                            col("year"), \
                            col("duration") \
                        ).distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_path = os.path.join(output_data, 'songs')
    songs_table.write.parquet(songs_path, mode='overwrite', partitionBy=["year", "artist_id"])

    # extract columns to create artists table
    artists_table = df.select(col("artist_id"), \
                                col("artist_name"), \
                                col("artist_location"), \
                                col("artist_latitude"), \
                                col("artist_longitude") \
                            ).distinct()

    # write artists table to parquet files
    artists_path = os.path.join(output_data, 'artists')
    artists_table.write.parquet(artists_path, mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Create users, time and songplays tables
    Load log files and input tables from S3
    Process data into output tables and write to partitioned parquet files on S3

    PARAMS: 
    spark : an active Spark session
    input_data : path to S3 bucket with input data
    output_data : path to S3 bucket to store output tables

    RETURNS: None
    """

    # get filepath to log data file
    log_data = "{}*/*/*events.json".format(input_data)

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    user_table = df.select(col("firstName"), \
                            col("lastName"), \
                            col("gender"), \
                            col("level"), \
                            col("userId") \
                        ).distinct()

    # write users table to parquet files
    users_path = os.path.join(output_data, 'users')
    users_table.write.parquet(users_path, mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("timestamp", get_timestamp(col("ts")))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: to_date(x), TimestampType())
    df = df.withColumn("start_time", get_timestamp(col("ts")))

    # extract columns to create time table
    df = df.withColumn("hour", hour("timestamp"))
    df = df.withColumn("day", dayofmonth("timestamp"))
    df = df.withColumn("month", month("timestamp"))
    df = df.withColumn("year", year("timestamp"))
    df = df.withColumn("week", weekofyear("timestamp"))
    df = df.withColumn("weekday", dayofweek("timestamp"))

    time_table = df.select(col("start_time"), \
                            col("hour"), \
                            col("day"), \
                            col("week"), \
                            col("month"), \
                            col("year"), \
                            col("weekday") \
                        ).distinct()

    # write time table to parquet files partitioned by year and month
    time_path = os.path.join(output_data, 'time')
    time_table.write.parquet(time_path, mode='append', partitionBy=["year", "month"])

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT DISTINCT song_id, artist_id, artist_name FROM song_df_table")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, song_df.artist_name == df.artist, "inner") \
        .select(col("start_time"), \
                col("userId"), \
                col("level"), \
                col("sessionId"), \
                col("location"), \
                col("userAgent"), \
                col("song_id"), \
                col("artist_id")) \
        .withColumn("songplay_id", monotonically_increasing_id() \
        ).distinct()

    # write songplays table to parquet files partitioned by year and month

    songplays_path = os.path.join(output_data, 'songplays')
    songplays_table.write.parquet(songplays_path, mode='overwrite', partitionBy=["year", "month"])


def main():
    """
    This is the main function
    
    PARAMS:
    
    RETURNS:
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
