# Databricks notebook source
# MAGIC %fs
# MAGIC ls FileStore/tables/

# COMMAND ----------

# DBTITLE 1,EDA & Transformation
df = spark.read.option('multiline', 'true').json('dbfs:/FileStore/tables/Streaming_History_Audio_2019_2024.json')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# No. of Columns in df dataset
len(df.columns)

# COMMAND ----------

# Total no of records in dataset.
df.count()

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumnRenamed('master_metadata_album_album_name' ,'album_name')\
    .withColumnRenamed('master_metadata_album_artist_name', 'artist_name')\
        .withColumnRenamed('master_metadata_track_name', 'track_name')
df.display()

# COMMAND ----------

df.select('platform').distinct().show()

# COMMAND ----------

from pyspark.sql.functions import lower, when
df = df.withColumn('platform',
                   when(lower('platform').contains('windows'), 'Windows')\
                       .when(lower('platform').contains('android'), 'Android')\
                           .otherwise(None)
                           )
df.display()

# COMMAND ----------

# Creating a new dataset by considering only required features.
sp_df = df.select(['album_name', 'artist_name', 'track_name', 'ms_played', 'platform', 'reason_end', 'reason_start', 'shuffle', 'skipped', 'ts'])

# COMMAND ----------

len(sp_df.columns)

# COMMAND ----------

# No. of records in dataset
sp_df.count()

# COMMAND ----------

sp_df.display()

# COMMAND ----------

#Displaying the count of NULL values in each Column.
from pyspark.sql.functions import col, when, count, coalesce
df_null_count = sp_df.select([count(when(col(c).isNull(),c)).alias(c) for c in sp_df.columns])
df_null_count.display()
# A Dictonary with columns and count of null values.
df_null_count_dict = {c : sp_df.filter(col(c).isNull()).count() for c in sp_df.columns}
df_null_count_dict

# COMMAND ----------

sp_df = sp_df.dropna(subset=['album_name', 'artist_name', 'track_name'])

# COMMAND ----------

# Replacing the NULL values to False
#Method 1 - Using WHEN function.
sp_df = sp_df.withColumn('skipped', when(col('skipped').isNull(), False).otherwise(col('skipped')))

#Method 2 - Using COALESCE function.
# sp_df = sp_df.withColumn('skipped', coalesce(col('skipped'), lit(False)))

# COMMAND ----------

from pyspark.sql.functions import col, when, count, coalesce
df_null_count = sp_df.select([count(when(col(c).isNull(),c)).alias(c) for c in sp_df.columns])
df_null_count.display()

# COMMAND ----------

sp_df = sp_df.withColumn('date', col('ts').cast('date'))
sp_df.display()

# COMMAND ----------

from pyspark.sql.functions import date_format
sp_df = sp_df.withColumn('year', date_format('date', 'y'))\
        .withColumn('month', date_format('date', 'MMMM'))\
            .withColumn('day', date_format('date', 'EEEE'))
sp_df.display()

# COMMAND ----------

from pyspark.sql.types import IntegerType
sp_df = sp_df.withColumn('month_num', date_format(col('ts'), 'M'))
sp_df = sp_df.withColumn('day_num',
                 when(col('day')=='Sunday', 1)\
                     .when(col('day')=='Monday', 2)\
                         .when(col('day')=='Tuesday', 3)\
                             .when(col('day')=='Wednesday', 4)\
                                 .when(col('day')=='Thursday', 5)\
                                     .when(col('day')=='Friday', 6)\
                                         .when(col('day')=='Saturday', 7)\
                                             .otherwise(None))
sp_df = sp_df.withColumn('hour', date_format(col('ts'), 'H'))
sp_df = sp_df.withColumn('year', col('year').cast(IntegerType()))\
    .withColumn('month_num', col('month_num').cast(IntegerType()))\
        .withColumn('hour', col('hour').cast(IntegerType()))
sp_df.display()

# COMMAND ----------

sp_df.printSchema()

# COMMAND ----------

sp_df.groupBy('reason_end').count().orderBy('count', ascending = False).show()
sp_df.groupBy('shuffle').count().orderBy('count', ascending = False).show()

# COMMAND ----------

# DBTITLE 1,Analysis Part
#Creating a view of the dataframe to run sql queries on it.
sp_df.createOrReplaceTempView('spotify')

# COMMAND ----------

#No.of Tracks I played.
Total_tracks_played = spark.sql('SELECT count(*) as tracks_played FROM spotify')
Total_tracks_played.display()

# COMMAND ----------

#NO. of songs i've listened to.
unique_songs_played = spark.sql('SELECT count(*) as songs_played FROM\
    (SELECT artist_name, track_name, COUNT(*) as cnt FROM spotify\
        GROUP BY artist_name, track_name)')
unique_songs_played.display()

# COMMAND ----------

#Total No.of Artists I've listened to.
total_artists = spark.sql('SELECT count(DISTINCT artist_name) as Total_Artists FROM spotify')
total_artists.display()

# COMMAND ----------

#No.of Times I've Listened to an Artist.
# TOP 10 Artists
artist_count = spark.sql('SELECT artist_name, count(*) as cnt FROM spotify\
        GROUP BY artist_name\
            ORDER BY cnt DESC\
                LIMIT 10')
artist_count.display()

# COMMAND ----------

#No. of songs i've listened of an artist
#TOP 10 Artists
artist_songs_played = spark.sql('SELECT artist_name, count(DISTINCT track_name) as songs FROM spotify\
    GROUP BY artist_name\
        ORDER BY songs DESC\
            LIMIT 10')
artist_songs_played.display()

# COMMAND ----------

#No.of Times I've listened to the track of an artist.
artist_track_count = spark.sql('SELECT concat(artist_name, " - ",track_name ) as Song, COUNT(*) as Times_played FROM spotify\
        GROUP BY artist_name, track_name\
            ORDER BY Times_played DESC\
                LIMIT 10')
artist_track_count.display()

# COMMAND ----------

#How many hours I've spent listening to an artist.
artist_time_spent = spark.sql('SELECT artist_name, round((sum(ms_played)/1000)/60, 3) as time_played_min , \
            round(((sum(ms_played)/1000)/60)/60, 1) as time_played_hr FROM spotify\
                GROUP BY artist_name\
                    ORDER BY time_played_min DESC\
                        LIMIT 10')
artist_time_spent.display()

# COMMAND ----------

#How many hours I've spent listening to the track of an artist.
artist_tract_time_spent = spark.sql('SELECT concat(artist_name, " - ",track_name ) as Song, sum(ms_played) as ms_played, \
    round((SUM(ms_played)/1000)/60, 3) as time_played_min, round(((SUM(ms_played)/1000)/60)/60, 2) as time_played_hr FROM spotify\
        GROUP BY artist_name, track_name\
            ORDER BY time_played_min DESC\
                LIMIT 10')
artist_tract_time_spent.display()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType
unique_artists = total_artists.collect()[0][0]
total_artists_count = sp_df.select('artist_name').count()
data = [('unique_artists', unique_artists), ('total_artists_count', total_artists_count)]
schema = StructType(
    [
        StructField('desc', StringType(), True),
        StructField('count', IntegerType(), True)
    ]
)
uniqueVsTotal_artists_df = spark.createDataFrame(data, schema)
uniqueVsTotal_artists_df.display()

# COMMAND ----------

unique_songs_count = unique_songs_played.collect()[0][0]
total_songs_count = Total_tracks_played.collect()[0][0]

data2 = [('unique_songs_count', unique_songs_count), ('total_songs_count', total_songs_count)]
schema2 = StructType(
    [
        StructField('desc', StringType(), True),
        StructField('count', IntegerType(), True)
    ]
)

uniqueVsTotal_songs_df = spark.createDataFrame(data2, schema2)
uniqueVsTotal_songs_df.display()

# COMMAND ----------

total_time_spent = spark.sql('SELECT round(((SUM(ms_played)/1000)/60)/60) as time_played_hr FROM spotify')
total_time_spent.display()

# COMMAND ----------

count_year_wise = spark.sql('SELECT year, count(*) as songs_count FROM spotify\
    GROUP BY year \
        ORDER BY year')
count_year_wise.display()

# COMMAND ----------

time_year_wise = spark.sql('SELECT year, round(((SUM(ms_played)/1000)/60)/60) as ms_played_hr FROM spotify\
    GROUP BY year\
        ORDER BY year')
time_year_wise.display()

# COMMAND ----------


tracksplayed_by_date = spark.sql('SELECT concat(date_format(date, "d"), date_format(date, "-MMMM"), date_format(date, "-y") ) as date, count(DISTINCT track_name) as tracks_played FROM spotify\
    GROUP BY date\
        ORDER BY tracks_played DESC\
            LIMIT 1')
tracksplayed_by_date.display()

# COMMAND ----------

timespent_by_date = spark.sql('SELECT concat(date_format(date, "d"), date_format(date, "-MMMM"), date_format(date, "-y") ) as date, date_format(date, "EEEE" ), round(((SUM(ms_played)/1000)/60)/60, 2) as ms_played_hr FROM spotify\
    GROUP BY date\
        ORDER BY ms_played_hr DESC\
            LIMIT 1 ')
timespent_by_date.display()

# COMMAND ----------

timespent_by_date2 = spark.sql('SELECT date, round(((SUM(ms_played)/1000)/60)/60, 2) as ms_played_hr FROM spotify\
    GROUP BY date\
        HAVING year(date) in (2023, 2024)\
        ORDER BY date')
timespent_by_date2.display()

# COMMAND ----------

# grouping with day column.
tracksplayed_by_day = spark.sql(
    'SELECT day, count(*) as tracksplayed FROM spotify\
        GROUP BY day,day_num\
            ORDER BY day_num'
)
tracksplayed_by_day.display()

# COMMAND ----------

timespent_by_day = spark.sql(
    'SELECT day, round(((SUM(ms_played)/1000)/60)/60, 1) as timeplayed FROM spotify\
        GROUP BY day, day_num\
            ORDER BY day_num'
)
timespent_by_day.display()

# COMMAND ----------

tracksplayed_by_month = spark.sql(
    'SELECT month, count(*) as tracksplayed FROM spotify\
        GROUP BY month, month_num\
            ORDER BY month_num'
)
tracksplayed_by_month.display()

# COMMAND ----------

timespent_by_month = spark.sql(
    'SELECT month, round(((SUM(ms_played)/1000)/60)/60, 1) as timeplayed FROM spotify\
        GROUP BY month, month_num\
            ORDER BY month_num'
)
timespent_by_month.display()

# COMMAND ----------

# groping with Platform feature.
tracksplayed_InDevice = spark.sql('SELECT platform, count(*) as tracks_played FROM spotify\
    GROUP BY platform\
        ORDER BY tracks_played DESC')
tracksplayed_InDevice.display()

# COMMAND ----------

timespent_InDevice = spark.sql('SELECT platform, round((sum(ms_played)/1000)/60/60, 1) as time_hr FROM spotify\
    GROUP BY platform\
        ORDER BY time_hr DESC')
timespent_InDevice.display()

# COMMAND ----------

# Top 5 Artists and their top 3 songs i've listened.

# COMMAND ----------

spark.sql(
    'SELECT day, hour, round((sum(ms_played)/1000)/60) as min_played FROM spotify\
        GROUP BY day, day_num, hour\
            ORDER BY day_num, hour'
).display()

# COMMAND ----------

hour_wise = spark.sql('SELECT hour, count(*) as tracks_played FROM spotify\
    GROUP BY hour\
        ORDER BY hour')
hour_wise.display()

# COMMAND ----------

#TOP 3 Artists of every year based on no.of tracks and time.

# COMMAND ----------

#Top 3 Tracks of every year based on no.of times and played hours.

# COMMAND ----------

# Average time of track played - SUM of total time played/Total No.of Tracks
# Average tracks played in a day\
    #1. Total No.of Tracks/ Total No.of Dates\
    #2. Total No.of Tracks / (Diff of days from start date to end date)
                                

# COMMAND ----------

#command line
