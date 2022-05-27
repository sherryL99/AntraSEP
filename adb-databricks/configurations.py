# Databricks notebook source
username = "ls_shirley77"

# COMMAND ----------


classicPipelinePath = f"/adbfinal/{username}/de/classic/"
rawPath = classicPipelinePath + "raw/"


# COMMAND ----------

bronzePath_movies = classicPipelinePath + "bronzeMovies/"
bronzePath_genres = classicPipelinePath + "bronzeGenres/"
bronzePath_languages = classicPipelinePath + "bronzeLanguages/"
silverPath_movies = classicPipelinePath + "silverMovies/"
silverPath_genres = classicPipelinePath + "silverGenres/"
silverPath_languages = classicPipelinePath + "silverLanguages/"


# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS adbfinal_{username}")
spark.sql(f"USE adbfinal_{username}")

# COMMAND ----------


