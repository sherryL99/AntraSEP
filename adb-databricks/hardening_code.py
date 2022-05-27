# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
    explode,
    abs,
    format_string
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

def readFile(container,filename:str) -> DataFrame:
    inputFilePath = "dbfs:/mnt/{}/{}".format(container,filename)
    df=spark.read.option("inferschema","true")\
    .option("multiline","true")\
    .format("json")\
    .load(inputFilePath)
    return df


# COMMAND ----------

def flattenDataFrame(dataframe: DataFrame) -> DataFrame:
    df_raw_exploded = dataframe.select(explode(dataframe["movie"]).alias("movies"))
    return df_raw_exploded 

# COMMAND ----------


import hashlib
import uuid

@udf
def compute_uuid(name: str) -> uuid.UUID:
    digest = hashlib.md5(name.encode()).digest()
    return uuid.UUID(bytes=digest)


# COMMAND ----------

def transform_raw_movie(raw: DataFrame) -> DataFrame:
    return raw.select(
        "movies",
  col("movies.CreatedDate").cast("date").alias("CreatedDate"),
  lit("new").alias("status"),
  current_timestamp().cast("date").alias("p_UpdatedDate")
    ).withColumn("uuid",compute_uuid(col("movies.Title")))


# COMMAND ----------

def transform_raw_genres(raw: DataFrame) -> DataFrame:
    return raw.select(
        explode("movies.genres").alias("genres"),
  col("movies.CreatedDate").cast("date").alias("CreatedDate"),
  lit("new").alias("status"),
  current_timestamp().cast("date").alias("p_UpdatedDate"),
  "uuid"
    )


# COMMAND ----------

def transform_raw_languages(raw: DataFrame) -> DataFrame:
    return raw.select(
   "movies.OriginalLanguage",
  col("movies.CreatedDate").cast("date").alias("CreatedDate"),
  lit("new").alias("status"),
  current_timestamp().cast("date").alias("P_UpdatedDate"),
"uuid"
    )


# COMMAND ----------

def transform_movie_bronze(tablename: str, quarantine: bool = False) -> DataFrame:
    bronze = spark.read.table(tablename).filter("status='new'")
    bronzeAugmentedDF = bronze.select("movies","uuid","movies.*")
    movie_silver = bronzeAugmentedDF.select(
    "movies",\
    "uuid",\
    "BackdropUrl",\
    "Budget",\
    "CreatedBy",\
    col("CreatedDate").cast("date").alias("CreatedDate"),\
    col("Id").cast("string").alias("Id"),\
    "ImdbUrl",\
    "OriginalLanguage",\
    "PosterUrl",\
    "Price",\
    col("ReleaseDate").cast("date").alias("ReleaseDate"),\
    "Revenue",\
    "RunTime",\
    "Tagline",\
    "Title",\
    "TmdbUrl",\
    "UpdatedBy",\
    current_timestamp().cast("date").alias("p_UpdatedDate"),\
    "genres"
)

    return movie_silver


# COMMAND ----------

def transform_genres_bronze(tablename: str, quarantine: bool = False) -> DataFrame:
    genres_bronze_new = spark.read.table(tablename).filter("status='new'")
    genres_silver = genres_bronze_new.select("genres","uuid",                             col("genres.id").alias("genres_id"),col("genres.name").alias("genres_name"))
    genres_silver = genres_silver.select(
        "uuid",
        "genres_id",
        "genres_name",
        current_timestamp().cast("date").alias("p_UpdatedDate")
    )
    return genres_silver

# COMMAND ----------

def transform_languages_bronze(tablename: str, quarantine: bool = False) -> DataFrame:
    languages_bronze_new = spark.read.table("languages_classic_bronze").filter("status='new'")
    languages_silver = languages_bronze_new.select("uuid","OriginalLanguage")
    languages_silver = languages_silver.select("*",current_timestamp().cast("date").alias("p_UpdatedDate"))
    
    return  languages_silver

# COMMAND ----------

def generate_genres_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("genres.name IS NOT NULL"),
        dataframe.filter("genres.name IS NULL"),
    )

def generate_movie_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("movies.RunTime > 0").filter("movies.Budget >= 1000000"),
        dataframe.filter("movies.RunTime < 0"),
        dataframe.filter("movies.Budget < 1000000")
    )
def generate_languages_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("OriginalLanguage IS NOT NULL"),
        dataframe.filter("OriginalLanguage IS NULL"),
    )
    


# COMMAND ----------

def read_batch_bronze(bronzePath: str) -> DataFrame:
    return spark.read.format("delta").load(bronzePath)

# COMMAND ----------

def read_batch_delta(deltaPath: str) -> DataFrame:
    return spark.read.format("delta").load(deltaPath)

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "append",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )

# COMMAND ----------

def update_bronze_table_status(
    spark: SparkSession, bronzeTablePath: str, dataframe: DataFrame, status: str
) -> bool:

    bronzeTable = DeltaTable.forPath(spark, bronzeTablePath)
    dataframeAugmented = dataframe.dropDuplicates(['uuid']).withColumn("status", lit(status))

    update_match = "bronze.uuid = dataframe.uuid"
    update = {"status": "dataframe.status"}

    (
        bronzeTable.alias("bronze")
        .merge(dataframeAugmented.alias("dataframe"), update_match)
        .whenMatchedUpdate(set=update)
        .execute()
    )

    return True


# COMMAND ----------

def repair_quarantined_records(
    spark: SparkSession, bronzeTable: str, userTable: str
) -> DataFrame:
    bronzeQuarantinedDF = spark.read.table(bronzeTable).filter("status = 'quarantined'")
    bronzeQuarTransDF = transform_bronze(bronzeQuarantinedDF, quarantine=True).alias(
        "quarantine"
    )
    health_tracker_user_df = spark.read.table(userTable).alias("user")
    repairDF = bronzeQuarTransDF.join(
        health_tracker_user_df,
        bronzeQuarTransDF.device_id == health_tracker_user_df.user_id,
    )
    silverCleanedDF = repairDF.select(
        col("quarantine.value").alias("value"),
        col("user.device_id").cast("INTEGER").alias("device_id"),
        col("quarantine.steps").alias("steps"),
        col("quarantine.eventtime").alias("eventtime"),
        col("quarantine.name").alias("name"),
        col("quarantine.eventtime").cast("date").alias("p_eventdate"),
    )
    return silverCleanedDF

# COMMAND ----------

def movie_quarantine_repaired(spark: SparkSession,bronzeTable: str) -> DataFrame:
    movies_bronzeQ = transform_movie_bronze(bronzeTable).filter(col("status") == 'quarantined').drop("movies")
    movies_bronzeQ = movies_bronzeQ\
    .withColumn("RunTime",abs(movies_bronzeQ.RunTime))\
    .withColumn("Budget",lit('1000000'))\
    .withColumn("Budget",col("Budget").cast("double"))\
    .withColumn("CreatedDate",col("CreatedDate").cast("date"))\
    .withColumn("Id",col("Id").cast("string"))\
    .withColumn("ReleaseDate",col("ReleaseDate").cast("date"))\
    .drop("movies")
    return movies_bronzeQ

# COMMAND ----------

def genres_quarantine_repaired(spark: SparkSession,bronzeTable: str) -> DataFrame:
    df_genres_deduplicate=spark.read.table(bronzeTable)\
    .dropDuplicates(['genres'])\
    .select('genres')

    genres_bronzeQuarantined = spark.read.table("genres_classic_bronze").filter(col("status") == 'quarantined')

    genres_bronze_repaired =genres_bronzeQuarantined.join(df_genres_deduplicate, genres_bronzeQuarantined.genres.id == df_genres_deduplicate.genres.id)\
    .drop(genres_bronzeQuarantined.genres)\
                             .select("uuid",\
                             col("genres.id").alias("genres_id"),\
                             col("genres.name").alias("genres_name"),\
                             "p_UpdatedDate")
    return genres_bronze_repaired 

# COMMAND ----------

def registerTable(spark: SparkSession, tableName:str, bronzePath:str):
    spark.sql(f"""
    drop table if exists {tableName}
    """)

    spark.sql(f"""
    create table {tableName}
    using Delta
    location "{bronzePath}"
    """)

# COMMAND ----------

# MAGIC %run ../configurations

# COMMAND ----------


def raw_bronze_silver(num:str,  partition_column: "p_UpdatedDate", mode:"append"): 
#raw to bronze
    df = readFile("newcontainer",f"movie_{num}.json".format(num))
    df_raw_exploded = flattenDataFrame(df)
    movie_bronze_uuid = transform_raw_movie(df_raw_exploded)
    genres_bronze = transform_raw_genres(movie_bronze_uuid)
    language_bronze = transform_raw_languages(movie_bronze_uuid)
    #dbutils.fs.rm(bronzePath_movies, recurse = True)
    bronzeToSilverWrite=batch_writer(movie_bronze_uuid,partition_column,mode)
    bronzeToSilverWrite.save(bronzePath_movies)
    #dbutils.fs.rm(bronzePath_genres, recurse = True)
    bronzeToSilverWrite=batch_writer(genres_bronze,partition_column,mode)
    bronzeToSilverWrite.save(bronzePath_genres)
   # dbutils.fs.rm(bronzePath_languages, recurse = True)
    bronzeToSilverWrite=batch_writer(language_bronze,partition_column,mode)
    bronzeToSilverWrite.save(bronzePath_languages)
    
    #Register Table
    registerTable(spark, 'movie_classic_bronze', bronzePath_movies)
    registerTable(spark, 'genres_classic_bronze', bronzePath_genres)
    registerTable(spark, 'languages_classic_bronze', bronzePath_languages)
    
#bronze to silver
    movie_silver = transform_movie_bronze("movie_classic_bronze")
    genres_silver = transform_genres_bronze("genres_classic_bronze")
    languages_silver = transform_languages_bronze("languages_classic_bronze")
    #isolate clean and quarantine
    movies_silver_clean, movies_silver_quarantine1, movies_silver_quarantine2 = generate_movie_clean_and_quarantine_dataframes(movie_silver)
    genres_silver_clean,genres_silver_quarantine= generate_genres_clean_and_quarantine_dataframes(genres_silver)
    languages_silver_clean,languages_silver_quarantine= generate_languages_clean_and_quarantine_dataframes(languages_silver)
    #dbutils.fs.rm(silverPath_movies, recurse=True)
    #dbutils.fs.rm(silverPath_genres, recurse=True)
    #dbutils.fs.rm(silverPath_languages, recurse=True)
    batch_writer(movies_silver_clean,partition_column,mode).save(silverPath_movies)
    batch_writer(genres_silver_clean,partition_column,mode).save(silverPath_genres)
    batch_writer(languages_silver_clean,partition_column,mode).save(silverPath_languages)
    
    #Register silver table
    registerTable(spark, 'movie_classic_silver', silverPath_movies)
    registerTable(spark, 'genres_classic_silver', silverPath_genres)
    registerTable(spark, 'languages_classic_silver', silverPath_languages)
    
    #change status of clean->load, label quarantine
    update_bronze_table_status(spark,bronzePath_movies,movies_silver_quarantine1,"quarantined")
    update_bronze_table_status(spark,bronzePath_movies,movies_silver_quarantine2,"quarantined")
    update_bronze_table_status(spark,bronzePath_genres,genres_silver_quarantine,"quarantined")
    update_bronze_table_status(spark,bronzePath_languages,languages_silver_quarantine,"quarantined")
    update_bronze_table_status(spark,bronzePath_movies,movies_silver_clean,"loaded")
    update_bronze_table_status(spark,bronzePath_genres,genres_silver_clean,"loaded")
    update_bronze_table_status(spark,bronzePath_languages,languages_silver_clean,"loaded")
    # quarantine repair
    movies_bronze_repaired = movie_quarantine_repaired(spark, "movie_classic_bronze")
    genres_bronze_repaired = genres_quarantine_repaired(spark, "genres_classic_bronze")
    batch_writer(movies_bronze_repaired,partition_column,mode).save(silverPath_movies)
    batch_writer(genres_bronze_repaired,partition_column,mode).save(silverPath_genres)
    update_bronze_table_status(spark,bronzePath_movies,movies_bronze_repaired,"loaded")
    update_bronze_table_status(spark,bronzePath_genres,genres_bronze_repaired,"loaded")
    update_bronze_table_status(spark,bronzePath_languages,languages_silver_quarantine,"loaded")
    update_bronze_table_status(spark,bronzePath_languages,languages_silver_quarantine,"loaded")
    
   # display(movies_silver_quarantine1)
   # display(movies_silver_quarantine2)
   # display(genres_silver_quarantine)

# COMMAND ----------

l =[0,1,2,3,4,5,6,7]

# COMMAND ----------

for i in l:
    raw_bronze_silver(num = i,partition_column= "p_UpdatedDate", mode ="append")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  genres_classic_silver where genres_name IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  movie_classic_silver where Budget < 1000000

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  languages_classic_silver

# COMMAND ----------

|
