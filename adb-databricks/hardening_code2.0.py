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

def raw_to_bronze(df:DataFrame):
    dataframe =  df.select(
  "movie",
  current_timestamp().cast("date").alias("CreatedDate"),
  lit("new").alias("status"),
  current_timestamp().cast("date").alias("p_UpdatedDate"))
    return dataframe


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

def transform_bronze_movie(raw: DataFrame) -> DataFrame:
    return raw.select(
        "movies",
  col("movies.CreatedDate").cast("date").alias("CreatedDate"),
  lit("new").alias("status"),
  current_timestamp().cast("date").alias("p_UpdatedDate")
    ).withColumn("uuid",compute_uuid(col("movies.Title")))


# COMMAND ----------

def transform_bronze_genres(raw: DataFrame) -> DataFrame:
    return raw.select(
        explode("movies.genres").alias("genres"),
  col("movies.CreatedDate").cast("date").alias("CreatedDate"),
  lit("new").alias("status"),
  current_timestamp().cast("date").alias("p_UpdatedDate"),
  "uuid"
    )


# COMMAND ----------

def transform_bronze_languages(raw: DataFrame) -> DataFrame:
    return raw.select(
   "movies.OriginalLanguage",
  col("movies.CreatedDate").cast("date").alias("CreatedDate"),
  lit("new").alias("status"),
  current_timestamp().cast("date").alias("P_UpdatedDate"),
"uuid"
    )


# COMMAND ----------

def bronze_to_silver(tablename: str) -> DataFrame:
    bronze = spark.read.table(tablename).filter("status='new'")
    
    movie_silver= bronze.select("movies","uuid","movies.*",current_timestamp().cast("date").alias("p_UpdatedDate")).withColumn("CreatedDate",col("CreatedDate").cast("date"))\
.withColumn("Id",col("Id").cast("string"))\
.withColumn("ReleaseDate",col("ReleaseDate").cast("date"))\
.withColumn("UpdatedDate",col("UpdatedDate").cast("date")).drop("movies")

    genres_silver = bronze.select(col("movies.genres").cast("string").alias("genres"),\
                                         "uuid",           explode("movies.genres").alias("nest_json_genres")).select("genres","uuid",col("nest_json_genres.id").alias("genres_id"),col("nest_json_genres.name").alias("genres_name"),current_timestamp().cast("date").alias("p_UpdatedDate"))
    languages_silver = bronze.select("uuid","movies.OriginalLanguage",current_timestamp().cast("date").alias("p_UpdatedDate"))
    
    return movie_silver, genres_silver, languages_silver


# COMMAND ----------

def generate_genres_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("genres_name IS NOT NULL"),
        dataframe.filter("genres_name IS NULL"),
    )

def generate_movie_clean_and_quarantine_dataframes(
    dataframe: DataFrame,
) -> (DataFrame, DataFrame):
    return (
        dataframe.filter("RunTime > 0").filter("Budget >= 1000000"),
        dataframe.filter("RunTime < 0"),
        dataframe.filter("Budget < 1000000")
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
            *exclude_columns)
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

def movie_quarantine_repaired(spark: SparkSession,bronzeTable:str) -> DataFrame:
    movies_silver = bronze_to_silver(bronzeTable)[0]
    _,movies_silver_quarantine1,movies_silver_quarantine2= generate_movie_clean_and_quarantine_dataframes(movies_silver) 
    movies_silver_quarantine1=movies_silver_quarantine1.select("*").withColumn("RunTime",abs(movies_silver_quarantine1.RunTime))\
    .withColumn("CreatedDate",col("CreatedDate").cast("date"))\
    .withColumn("Id",col("Id").cast("string"))\
    .withColumn("ReleaseDate",col("ReleaseDate").cast("date"))\
    .withColumn("UpdatedDate",col("UpdatedDate").cast("date")).drop("movies")

    movies_silver_quarantine2=movies_silver_quarantine2\
    .withColumn("Budget",lit('1000000'))\
    .withColumn("Budget",col("Budget").cast("double"))\
    .withColumn("CreatedDate",col("CreatedDate").cast("date"))\
    .withColumn("Id",col("Id").cast("string"))\
    .withColumn("ReleaseDate",col("ReleaseDate").cast("date"))\
    .withColumn("UpdatedDate",col("UpdatedDate").cast("date")).drop("movies")
    
    movies_bronze_repaired = movies_silver_quarantine1.union(movies_silver_quarantine2)
    return movies_bronze_repaired

# COMMAND ----------

def genres_quarantine_repaired(spark: SparkSession,bronzeTable: str) -> DataFrame:
    bronze = bronze_to_silver(bronzeTable)[1]
    genres_silver_clean,genres_silver_quarantine = generate_genres_clean_and_quarantine_dataframes(bronze)
    df_genres_deduplicate = spark.read.table(bronzeTable)\
    .select(explode("movies.genres")).dropDuplicates(['col'])
  
    genres_bronze_repaired =genres_silver_quarantine.join(df_genres_deduplicate,genres_silver_quarantine.genres_id == df_genres_deduplicate.col.id)
    
    genres_bronze_repaired = genres_bronze_repaired.select("uuid",\
                             col("col.id").alias("genres_id"),\
                             col("col.name").alias("genres_name"),\
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

dbutils.fs.rm(bronzePath_movies, recurse = True)
dbutils.fs.rm(silverPath_movies, recurse=True)
dbutils.fs.rm(silverPath_genres, recurse=True)
dbutils.fs.rm(silverPath_languages, recurse=True)
def raw_bronze_silver(num:str,  partition_column: "p_UpdatedDate", mode:"append"): 
    #raw to bronze
    df = readFile("newcontainer",f"movie_{num}.json".format(num))
    df = raw_to_bronze(df)
    #bronze to silver
    df_raw_exploded = flattenDataFrame(df)
    movie_bronze_uuid = transform_bronze_movie(df_raw_exploded)
    genres_bronze = transform_bronze_genres(movie_bronze_uuid)
    language_bronze = transform_bronze_languages(movie_bronze_uuid)
    #dbutils.fs.rm(bronzePath_movies, recurse = True)
    bronzeToSilverWrite=batch_writer(movie_bronze_uuid,partition_column,mode)
    bronzeToSilverWrite.save(bronzePath_movies)
    #Register Table
    registerTable(spark, 'movie_classic_bronze', bronzePath_movies)

    movie_silver, genres_silver, languages_silver = bronze_to_silver("movie_classic_bronze")
    #isolate clean and quarantine
    movies_silver_clean, movies_silver_quarantine1, movies_silver_quarantine2 = generate_movie_clean_and_quarantine_dataframes(movie_silver)
    genres_silver_clean, genres_silver_quarantine= generate_genres_clean_and_quarantine_dataframes(genres_silver)
    languages_silver_clean, languages_silver_quarantine= generate_languages_clean_and_quarantine_dataframes(languages_silver)
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
    genres_bronze_repaired = genres_quarantine_repaired(spark, "movie_classic_bronze")
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
# MAGIC select count(*) from  movie_classic_bronze where status = 'quarantined'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from  movie_classic_bronze

# COMMAND ----------


