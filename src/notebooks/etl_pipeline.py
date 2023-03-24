# Databricks notebook source
# MAGIC %md ### Import Packages

# COMMAND ----------

from pyspark.sql.functions import datediff, avg
from pyspark.sql.functions import current_date
from pyspark.sql.types import IntegerType

# COMMAND ----------

# MAGIC %md ### Read in Dataset

# COMMAND ----------

# Use spark package to read the csv file from AWS
# You can go to the github repo for spark to read more about it
# Use the 'header' parameter to fix the header in the dataset when its not in the column head

df_laptimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header = True)

# COMMAND ----------

# View the data frame
display(df_laptimes)

# COMMAND ----------

# Read in the drivers dataset
df_drivers = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv', header = True)


# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md ### Transform Data

# COMMAND ----------

# 'withColumn' creates a new column, then we input the logic we will use to create that new column
# Make sure to load the functions 
df_drivers = df_drivers.withColumn("age", datediff(current_date(),df_drivers.dob)/365)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# Overwrite the 'age' column and cast it to type integer from a float which has decimals
df_drivers = df_drivers.withColumn("age", df_drivers['age'].cast(IntegerType()))

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# Join the laps and drivers data set together
# The select is only applied to the dirvers data set
# Select these columns from the dirvers data and then join it to lap times dataset on driverID column
df_lap_drivers = df_drivers.select('driverID', 'driverRef', 'code', 'forename', 'surname', 'nationality', 'age').join(df_laptimes, on=['driverId'])

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

df_races = spark.read.csv('s3://columbia-gr5069-main/raw/races.csv', header = True)

# COMMAND ----------

display(df_races)

# COMMAND ----------

# Reestantiate the dataset since that data is always saved to memory, just rerun the original build in this case df_lap_drivers

df_lap_drivers = df_lap_drivers.join(df_races.select('year', 'name', 'raceId'), on = ['raceId'])

# COMMAND ----------

# Remove the raceid and driverid columns
df_lap_drivers = df_lap_drivers.drop('raceId', 'driverID')

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md ### Aggregate by Age

# COMMAND ----------

# the agg is used for aggregation 
df_agg_age = df_lap_drivers.groupby('age').agg(avg('milliseconds'))

# COMMAND ----------

display(df_agg_age)

# COMMAND ----------

# MAGIC %md ### Storing Data in S3

# COMMAND ----------

# Store processed data in your processed folder of your bucket
# copy the url from the S3 bucket
# You create new folders by adding back slashes
df_agg_age.write.csv("s3://sla2172-gr5069/processed/inclass/laptimes_by_age.csv")
