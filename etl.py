import configparser
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extracts data about songs from S3, transforms it to dimensional tables and
    and loads the transformed data back to S3

    :param spark: spark session object
    :param input_data: path to the location of the input data on S3
    :param output_data: path to the location where the transformed data should be stored
    """
    # filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    # read song data file
    df_songs = spark.read.json(song_data)

    # define a table which contains the raw song data
    df_songs.createOrReplaceTempView('stage_songs')

    # define the SQL-query which transforms the raw song data into the songs dimension
    str_sql = """
    SELECT
        DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM
        stage_songs
    WHERE
        song_id IS NOT NULL
    """
    # define the songs dimension
    songs_table = spark.sql(str_sql)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(output_data + 'dim_songs/')

    # define SQL-query which transforms the raw data into the artists dimension
    str_sql = """
    SELECT
        DISTINCT artist_id,
        artist_name AS name,
        artist_location AS location,
        artist_latitude AS latitude,
        artist_longitude AS longitude
    FROM
        stage_songs
    WHERE
        artist_id IS NOT NULL
    """

    artists_table = spark.sql(str_sql)
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'dim_artists/')


def process_log_data(spark, input_data, output_data):
    """
    Extracts event logs from S3 and transforms them into fact and dimension tables which are loaded
    back to S3

    :param spark: spark session object
    :param input_data: path to the location of the input data on S3
    :param output_data: path to the location where the transformed data should be stored
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*.json'

    # read log data file and filter relevant records
    df_logs = spark.read.json(log_data)
    df_logs = df_logs.filter(df_logs.page == 'NextSong')

    # create a table to store the loaded raw log data
    df_logs.createOrReplaceTempView('stage_logs')
    
    # define SQL-query which derives from the raw log data the user dimension
    str_sql = """
    SELECT
        DISTINCT CAST(userId AS INT) AS user_id,
        firstName AS first_name,
        lastName AS last_name,
        gender,
        level 
    FROM
        stage_logs
    WHERE userId is NOT NULL
    """

    # define the user dimension
    users_table = spark.sql(str_sql)
    
    # write artist table to parquet files
    users_table.write.parquet(output_data + 'dim_users/')

    # define a udf which converts the unix timestamp to datetime
    def to_datetime(ts):
        return datetime.datetime.fromtimestamp(ts / 1000.0)

    # register the uds
    spark.udf.register("to_datetime", to_datetime, TimestampType())

    # define the SQL-query which include the start_time column in the raw log data
    str_sql = """
    SELECT
        *,
        to_datetime(ts) AS start_time
    FROM
        stage_logs
    """

    df_log_enh = spark.sql(str_sql)
    df_log_enh.createOrReplaceTempView('stage_logs')

    # to derive the songsplay fact table, we need both the song and artist dimensions
    songs_table = spark.read.parquet(output_data + 'dim_songs/*/*/*')
    songs_table.createOrReplaceTempView('dim_songs')

    artists_table = spark.read.parquet(output_data + 'dim_artists/*')
    artists_table.createOrReplaceTempView('dim_artists')

    # define SQL statement which looks up the artist_id for the songsplay fact table
    str_sql = """
    SELECT
        l.*,
        a.artist_id
    FROM
        stage_logs_enh AS l,
        dim_artists AS a
    WHERE a.name = l.artist
    """

    df_logs_enh_with_artist_id = spark.sql(str_sql)
    df_logs_enh_with_artist_id.createOrReplaceTempView('stage_logs_with_artist_id')

    # define SQL statement which looks up the song_id
    str_sql = """
    SELECT
        l.*,
        s.song_id
    FROM
        stage_logs_with_artist_id AS l,
        dim_songs AS s
    WHERE
        l.song = s.title 
    """

    df_log_enh_with_artist_and_song_id = spark.sql(str_sql)
    df_log_enh_with_artist_and_song_id.createOrReplaceTempView('stage_logs_with_artist_and_song_id')

    # define the final SQL-statement which derives the songsplay fact table
    str_sql = """
    SELECT
        monotonically_increasing_id() as songplay_id,
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        sessionId A session_id,
        location,
        userAgent AS user_agent
    FROM
       stage_logs_with_artist_and_song_id 
    """

    songplays_table = spark.sql(str_sql)

    # write songsplay table to S3
    songplays_table.write.partitionBy("year", "month").parquet(output_data + 'fact_songplays_table/')

    # define the time dimension
    str_sql = """
    SELECT
        DISTINCT datetime AS start_time,
        hour(datetime) AS hour,
        dayofmonth(datetime) AS day,
        weekofyear(datetime) AS week,
        month(datetime) AS month,
        year(datetime) AS year,
        dayofweek(datetime) AS weekday
    FROM
       stage_logs_with_artist_and_song_id 
    """

    time_table = spark.sql(str_sql)
    time_table.write.partitionBy("year", "month").parquet(output_data + 'dim_time/')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
