-- Databricks notebook source
-- MAGIC %md ##Different Delta Lake Table
-- MAGIC 
-- MAGIC #### Objectives
-- MAGIC By the end of this you will able to understand 
-- MAGIC - Different types of Delta Lake Tables
-- MAGIC - Difference between External and Managed Tables
-- MAGIC 
-- MAGIC More information on Delta Lake Tables are can be found here 
-- MAGIC https://learn.microsoft.com/en-gb/azure/databricks/delta/
-- MAGIC 
-- MAGIC Different create table options
-- MAGIC https://learn.microsoft.com/en-gb/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using

-- COMMAND ----------

-- MAGIC %md ##1. Managed Table

-- COMMAND ----------

-- MAGIC %md ###Create Table

-- COMMAND ----------

create table if not exists emp_managed
(
    id int,
    first_name String,
    last_name String    
) using delta

-- COMMAND ----------

describe extended emp_managed

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/emp_managed')

-- COMMAND ----------

-- MAGIC %md ### Insert some data

-- COMMAND ----------

insert into emp_managed values 
(1,'david','lee'),
(2, 'john','dow')

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/emp_managed')

-- COMMAND ----------

-- MAGIC %md ### drop the table

-- COMMAND ----------

drop table emp_managed

-- COMMAND ----------

-- MAGIC %md ### Dropping table also deletes the underlying files and path is also removed

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/emp_managed')

-- COMMAND ----------

-- MAGIC %md ##1. External Table

-- COMMAND ----------

-- MAGIC %md ### Create emp dataframe

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC data = [[1, "david", "lee"], 
-- MAGIC         [2, "john", "dow"]]
-- MAGIC   
-- MAGIC columns = ["id","first_name","last_name"]
-- MAGIC emp_df = spark.createDataFrame(data, columns)
-- MAGIC display(emp_df)

-- COMMAND ----------

-- MAGIC %md ###Save the dataframe as delta lake format

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC emp_df.write.format("delta").mode("overwrite").save('dbfs:/user/hive/warehouse/emp_external')

-- COMMAND ----------

-- MAGIC %md ### Check the files

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/emp_external')

-- COMMAND ----------

-- MAGIC %md ###Create the table using location

-- COMMAND ----------

create table emp_external 
using delta
location 'dbfs:/user/hive/warehouse/emp_external'

-- COMMAND ----------

select * from emp_external

-- COMMAND ----------

-- MAGIC %md ### Describe the table

-- COMMAND ----------

describe extended emp_external

-- COMMAND ----------

-- MAGIC %md ### Drop the external table

-- COMMAND ----------

drop table emp_external

-- COMMAND ----------

-- MAGIC %md ### Verify the files (Table is dropped but files remains)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls('dbfs:/user/hive/warehouse/emp_external')

-- COMMAND ----------


