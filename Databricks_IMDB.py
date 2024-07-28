# Databricks notebook source
print("hello")

# COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://cntnr@storage00110.blob.core.windows.net",
  mount_point = "/mnt/poc",
  extra_configs = {"fs.azure.account.key.storage00110.blob.core.windows.net":"cwsb3bfLpXhiWABLLJlaUjmMmiWrRGzWUHOE6KuC0UuUqVXL8CberW5JF8cVEYEe6S0Y/660lneu+AStpnGLKQ=="})

# COMMAND ----------

display(dbutils.fs.ls("/mnt/poc"))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/poc/raw"))

# COMMAND ----------

file1=spark.read.csv("/mnt/poc/raw/contentDataGenre.csv",header=True,inferSchema=True)
display(file1)

# COMMAND ----------

file2=spark.read.csv("/mnt/poc/raw/contentDataPrime.csv",header=True,inferSchema=True)
display(file2)

# COMMAND ----------

file3=spark.read.csv("/mnt/poc/raw/contentDataRegion.csv",header=True,inferSchema=True)
display(file3)

# COMMAND ----------

file2=file2.na.fill('NA')
file2=file2.filter(file2.title != 'NA')
file2= file2.withColumnRenamed("dataId","dataId2")
display(file2)

# COMMAND ----------

file3= file3.withColumnRenamed("dataId","dataId3")
display(file3)

# COMMAND ----------

join1=file1.join(file2,file1.dataId==file2.dataId2,"inner")
display(join1)

# COMMAND ----------

join1=join1.drop(join1.dataId2)
display(join1)

# COMMAND ----------

join2=join1.join(file3,join1.dataId==file3.dataId3)
display(join2)

# COMMAND ----------

join2=join2.drop(join2.dataId3)
display(join2)

# COMMAND ----------

join2.printSchema()

# COMMAND ----------

from pyspark.sql.functions import *

df=join2.withColumn("endYear", regexp_replace("endYear", "-1", "NA"))
df1=df.withColumn("releaseYear", regexp_replace("releaseYear", "-1", "NA"))
df1=df1.drop(df1.gross)
display(df1)

# COMMAND ----------

df1= df1.withColumnRenamed("dataId","Id") \
     .withColumnRenamed("genre","Genre")\
.withColumnRenamed("contentType","ContentType")\
.withColumnRenamed("title","Title")\
.withColumnRenamed("length","Length")\
.withColumnRenamed("releaseYear","ReleaseYear")\
.withColumnRenamed("endYear","EndYear")\
.withColumnRenamed("votes","Votes")\
.withColumnRenamed("rating","Rating")\
.withColumnRenamed("certificate","Certificate")\
.withColumnRenamed("description","Description")\
.withColumnRenamed("region","Region")
display(df1)

# COMMAND ----------

IMDBdata=df1

# COMMAND ----------

IMDBdata.write.mode('overwrite').saveAsTable("IMDBdata1")

# COMMAND ----------

IMDBdata.coalesce(1).write.csv("/mnt/poc/cleansed/IMDBdata.csv",header=True)

# COMMAND ----------


