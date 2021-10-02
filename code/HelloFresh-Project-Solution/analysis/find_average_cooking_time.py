# Databricks notebook source
spark.conf.set("set spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest recipes data

# COMMAND ----------

# MAGIC %run "../utils/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the Json file using the spark dataframe reader API and define schema on read dynamically

# COMMAND ----------

recepies_df_schema = [['cookTime','string','False'],['datePublished','date','False'],['description','string','False'],['image','string','False'],['ingredients','string','False'],['prepTime','string','False'],['recipeYield','string','False'],['url','string','False']]

# COMMAND ----------

recepies_df = spark.read \
.schema(convertToSparkSchema(recepies_df_schema))\
.json("/mnt/datalake1989/raw/hellofresh/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Filter Out records which has beef as an Ingredients.

# COMMAND ----------

recepies_with_beef_df = recepies_df.filter("ingredients like '%beef%'").cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Converting cookTime and prepTime In Seconds to calculate total cooking time.

# COMMAND ----------

from pyspark.sql import functions as func
from pyspark.sql.functions import *
cook_prep_time_seconds_df =recepies_with_beef_df\
.withColumn("cookTimeInSeconds",when(func.substring("cookTime",3,4).contains("H"),func.unix_timestamp(func.regexp_replace(func.substring("cookTime",3,4),"H|M",'h|m'),"H:mm")).otherwise(func.unix_timestamp(func.substring("cookTime",3,4),"mm")))\
.withColumn("prepTimeInSeconds",when(func.substring("prepTime",3,4).contains("H"),func.unix_timestamp(func.regexp_replace(func.substring("prepTime",3,4),"H|M",'h|m'),"H:mm")).otherwise(func.unix_timestamp(func.substring("prepTime",3,4),"mm")))

# COMMAND ----------

display(cook_prep_time_seconds_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Segregatting records based on total cook time interval in seconds to classify Easy, Medium and Hard levels.

# COMMAND ----------

total_cook_time_df = cook_prep_time_seconds_df.select(((col('cookTimeInSeconds') + col('prepTimeInSeconds'))).alias('total_cooking_time'))\
                      .withColumn("difficulty", when(col('total_cooking_time') < 1800,'Easy')\
                                    .otherwise(when( (col('total_cooking_time') >= 1800) & (col('total_cooking_time') < 3600),'Medium')\
                                    .otherwise('Hard')))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5 - Write to output to processed container in csv format as decribed.

# COMMAND ----------

total_cook_time_df.repartition(1).write.mode("overwrite").option("header",True).csv("/mnt/datalake1989/processed/hellofresh/")