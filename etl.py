import configparser
from datetime import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql import types


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    '''
      Initializes the Spark session with the required packages and leaves it ready for execution.
    '''
    spark = (SparkSession
        .builder
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate())
    return spark


def process_song_data(spark, input_data, output_data):
    '''
      Takes data from the transactional song files and transforms it into the songs and artists
      analytical tables. Steps are commented along the way, in summary these tables are dimmension
      tables that consist on a subset of columns with handled duplicates.
    '''
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.schema(types.StructType([
        types.StructField('num_songs', types.IntegerType(), True),
        types.StructField('artist_id', types.StringType(), True),
        types.StructField('artist_latitude', types.DoubleType(), True),
        types.StructField('artist_longitude', types.DoubleType(), True),
        types.StructField('artist_location', types.StringType(), True),
        types.StructField('artist_name', types.StringType(), True),
        types.StructField('song_id', types.StringType(), True),
        types.StructField('title', types.StringType(), True),
        types.StructField('duration', types.DoubleType(), True),
        types.StructField('year', types.IntegerType(), True),
    ])).json(song_data)

    # extract columns to create songs table
    songs_table = df.selectExpr([
        'song_id',
        'title',
        'artist_id',
        'year',
        'duration',
    ]).dropDuplicates(['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    (songs_table.write
     .partitionBy('year', 'artist_id')
     .parquet(os.path.join(output_data, 'songs')))

    # extract columns to create artists table
    artists_table = df.selectExpr([
        'artist_id',
        'artist_name      as name',
        'artist_location  as location',
        'artist_latitude  as latitude',
        'artist_longitude as longitude'
    ]).dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    (artists_table.write
     .parquet(os.path.join(output_data, 'artists')))

    
def process_log_data(spark, input_data, output_data):
    '''
      Takes data from the transactional log files and transforms it into the time and users
      analytical tables. Afterwards, the dataset is merged with the songs and artist tables
      in order to build the fact table songplays. Steps are commented along the way.
    '''
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.option('multiline', 'true').schema(types.StructType([
        types.StructField('artist', types.StringType(), True),
        types.StructField('auth', types.StringType(), True),
        types.StructField('firstName', types.StringType(), True),
        types.StructField('gender', types.StringType(), True),
        types.StructField('itemInSession', types.IntegerType(), True),
        types.StructField('lastName', types.StringType(), True),
        types.StructField('length', types.DoubleType(), True),
        types.StructField('level', types.StringType(), True),
        types.StructField('location', types.StringType(), True),
        types.StructField('method', types.StringType(), True),
        types.StructField('page', types.StringType(), True),
        types.StructField('registration', types.DoubleType(), True),
        types.StructField('sessionId', types.IntegerType(), True),
        types.StructField('song', types.StringType(), True),
        types.StructField('status', types.IntegerType(), True),
        types.StructField('ts', types.LongType(), True),
        types.StructField('userAgent', types.StringType(), True),
        types.StructField('userId', types.StringType(), True),
    ])).json(log_data)
    
    # filter by actions for song plays
    df = df.filter('page == "NextSong"')

    # extract columns for users table    
    users_table = df.selectExpr([
        'userId    as user_id',
        'firstName as first_name',
        'lastName  as last_name',
        'gender',
        'level',
    ]).dropDuplicates(['user_id'])
    
    # write users table to parquet files
    (users_table.write
     .parquet(os.path.join(output_data, 'users')))

    # create timestamp column from original timestamp column
    get_timestamp = udf(datetime.fromtimestamp, types.TimestampType())
    df = df.withColumn('timestamp', get_timestamp(col('ts') / 1000))
    
    # extract columns to create time table
    time_table = df.selectExpr([
        'timestamp             as start_time',
        'hour(timestamp)       as hour',
        'dayofmonth(timestamp) as day',
        'weekofyear(timestamp) as week',
        'month(timestamp)      as month',
        'year(timestamp)       as year',
        'dayofweek(timestamp)  as weekday',
    ]).dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    (time_table.write
     .partitionBy('year', 'month')
     .parquet(os.path.join(output_data, 'time')))

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs'))


    # IMPORTANT NOTE: In order to partition the parquet by year and month as requested
    #                 these two columns MUST be on the dataframe, even when this is not
    #                 on the requested schema.

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = (df.join(song_df, df.song == song_df.title)
                       .withColumn('songplay_id', monotonically_increasing_id())
                       .selectExpr([
                           'songplay_id',
                           'timestamp        as start_time',
                           'year(timestamp)  as year',
                           'month(timestamp) as month',
                           'userId           as user_id',
                           'level',
                           'song_id',
                           'artist_id',
                           'sessionId        as session_id',
                           'location',
                           'userAgent        as user_agent',
                       ]))

    # write songplays table to parquet files partitioned by year and month
    (songplays_table.write
     .partitionBy('year', 'month')
     .parquet(os.path.join(output_data, 'songplays')))

    
def main():
    '''
      The whole process is run.
    '''
    spark = create_spark_session()
    input_data = "s3://udacity-dend/"
    output_data = "s3://vlizanaudacitydatalakeoutput/analytics/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

    
if __name__ == "__main__":
    main()

