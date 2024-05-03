-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Analysis Part
-- MAGIC As the extracted data transformed into the required format in the other notebook, This Notebook mainly emphasis on the *Exploration* and *Visualization*.
-- MAGIC
-- MAGIC - For Analysis/Expoloration, I have written SQL queries which are executed on databricks own ***SQL Editor***, but not on this notebook.  
-- MAGIC This notebook is to put all queries in one place.  
-- MAGIC - For visualization, databricks provides its own visualization tool. That has been used instead of any external libraries for creation of charts and graphs.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##SQL Queries
-- MAGIC The main agenda of this Analysis is to address the following statements.
-- MAGIC 1. Total Tracks Played
-- MAGIC 2. Total Songs Listened
-- MAGIC 3. How many Artists I've listened to?
-- MAGIC 4. Most Often Listened Artists?
-- MAGIC 5. Which artist has the most songs that I have listened to?
-- MAGIC 6. Most Often Listened Songs
-- MAGIC 7. Which artist I've spent most of my time listening to on Spotify?
-- MAGIC 8.  Which song I've spent most of my time listening to on Spotify?
-- MAGIC 9. What is the Unique Songs Ratio?
-- MAGIC 10. How much time spent listening to music on spotify overall?
-- MAGIC 11. How much time(Hours) is spent on spotify each year?
-- MAGIC 12. How many songs are played each year?
-- MAGIC 13. Which day most tracks were played on?
-- MAGIC 14. Which day have I listened to spotify for the longest?
-- MAGIC 15. What is the 2023–2024 Listening Pattern like?
-- MAGIC 16. Which weekday has most played tracks?
-- MAGIC 17. Which month has most played tracks?
-- MAGIC 18. Which weekday has most listening hours?
-- MAGIC 19. Which month has most listening hours?
-- MAGIC 20. How many tracks are played on Android and Windows?
-- MAGIC 21. How much time listened to spotify on Android and Windows?
-- MAGIC 22. How much time did I spend listening to spotify on daily basis?
-- MAGIC 23. What time of day have I listened to Spotify the most?

-- COMMAND ----------

-- 1. Total Tracks Played
SELECT count(*) as tracks_played FROM spotify;

-- COMMAND ----------

-- 2. Total Songs Listened
SELECT count(*) as songs_played FROM
(SELECT artist_name, track_name, COUNT(*) as cnt FROM spotify
GROUP BY artist_name, track_name);

-- COMMAND ----------

-- 3. How many Artists I've listened to?
SELECT count(DISTINCT artist_name) as Total_Artists FROM spotify;

-- COMMAND ----------

-- 4. Most Often Listened Artists?
SELECT artist_name, count(*) as cnt FROM spotify
GROUP BY artist_name
ORDER BY cnt DESC
LIMIT 10

-- COMMAND ----------

-- 5. Which artist has the most songs that I have listened to?
SELECT artist_name, count(DISTINCT track_name) as songs FROM spotify
GROUP BY artist_name
ORDER BY songs DESC
LIMIT 10

-- COMMAND ----------

-- 6. Most Often Listened Songs
SELECT concat(artist_name, " - ",track_name ) as Song, COUNT(*) as Times_played FROM spotify
GROUP BY artist_name, track_name
ORDER BY Times_played DESC
LIMIT 10

-- COMMAND ----------

-- 7. Which artist I've spent most of my time listening to on Spotify?
SELECT artist_name, round(((sum(ms_played)/1000)/60)/60, 1) as time_played_hr FROM spotify
GROUP BY artist_name
ORDER BY time_played_hr DESC
LIMIT 10

-- COMMAND ----------

-- 8.  Which song I've spent most of my time listening to on Spotify?
SELECT concat(artist_name, " - ",track_name ) as Song, round((SUM(ms_played)/1000)/60, 3) as time_played_min, 
round(((SUM(ms_played)/1000)/60)/60, 2) as time_played_hr FROM spotify
GROUP BY artist_name, track_name
ORDER BY time_played_min DESC
LIMIT 10

-- COMMAND ----------

-- 9. What is the Unique Songs Ratio?
SELECT 'Total Records' as description ,count(*) as value FROM spotify
UNION
SELECT 'Unique Songs', count(*) as value FROM
    (SELECT artist_name, track_name, COUNT(*) as cnt FROM spotify
        GROUP BY artist_name, track_name)

-- COMMAND ----------

--10. How much time spent listening to music on spotify overall?
SELECT round(((SUM(ms_played)/1000)/60)/60) as time_played_hr FROM spotify

-- COMMAND ----------

-- 11. How much time(Hours) is spent on spotify each year?
SELECT year, count(*) as songs_count FROM spotify
GROUP BY year 
ORDER BY year

-- COMMAND ----------

-- 12. How many songs are played each year?
SELECT year, round(((SUM(ms_played)/1000)/60)/60) as ms_played_hr FROM spotify
GROUP BY year
ORDER BY year

-- COMMAND ----------

-- 13. Which day most tracks were played on?
SELECT concat(date_format(date, "d"), date_format(date, "-MMMM"), date_format(date, "-y") ) as date, count(track_name) as tracks_played FROM spotify
GROUP BY date
ORDER BY tracks_played DESC
LIMIT 1

-- COMMAND ----------

-- 14. Which day have I listened to spotify for the longest?
SELECT concat(date_format(date, "d"), date_format(date, "-MMMM"), date_format(date, "-y") ) as date, date_format(date, "EEEE" ), 
round(((SUM(ms_played)/1000)/60)/60, 2) as ms_played_hr FROM spotify
GROUP BY date
ORDER BY ms_played_hr DESC
LIMIT 1

-- COMMAND ----------

-- 15. What is the 2023–2024 Listening Pattern like?
SELECT date, round(((SUM(ms_played)/1000)/60)/60, 2) as ms_played_hr FROM spotify
GROUP BY date
HAVING year(date) in (2023, 2024)
ORDER BY date

-- COMMAND ----------

-- 16. Which weekday has most played tracks?
SELECT day, count(*) as tracksplayed FROM spotify
GROUP BY day,day_num
ORDER BY day_num

-- COMMAND ----------

-- 17. Which month has most played tracks?
SELECT day, round(((SUM(ms_played)/1000)/60)/60, 1) as timeplayed FROM spotify
GROUP BY day, day_num
ORDER BY day_num

-- COMMAND ----------

-- 18. Which weekday has most listening hours?
SELECT month, count(*) as tracksplayed FROM spotify
GROUP BY month, month_num
ORDER BY month_num

-- COMMAND ----------

--19. Which month has most listening hours?
SELECT month, round(((SUM(ms_played)/1000)/60)/60, 1) as timeplayed FROM spotify
GROUP BY month, month_num
ORDER BY month_num

-- COMMAND ----------

-- 20. How many tracks are played on Android and Windows?
SELECT platform, count(*) as tracks_played FROM spotify
GROUP BY platform
ORDER BY tracks_played DESC

-- COMMAND ----------

-- 21. How much time listened to spotify on Android and Windows?
SELECT platform, round((sum(ms_played)/1000)/60/60, 1) as time_hr FROM spotify
GROUP BY platform
ORDER BY time_hr DESC

-- COMMAND ----------

-- 22. How much time did I spend listening to spotify on daily basis?
SELECT day, hour, round((sum(ms_played)/1000)/60) as min_played FROM spotify
GROUP BY day, day_num, hour
ORDER BY day_num, hour

-- COMMAND ----------

-- 23. What time of day have I listened to Spotify the most?
SELECT hour, round((sum(ms_played)/1000)/60) as min_played FROM spotify
GROUP BY hour
ORDER BY hour

-- COMMAND ----------

-- MAGIC %md
-- MAGIC This is the End of this Notebook.
