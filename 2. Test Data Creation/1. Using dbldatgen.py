# Databricks notebook source
# MAGIC %md ### Test Data generation using dbldatgen
# MAGIC 
# MAGIC For more information refere documentation
# MAGIC  - https://databrickslabs.github.io/dbldatagen/public_docs/
# MAGIC  
# MAGIC GIT link
# MAGIC - https://github.com/databrickslabs/dbldatagen

# COMMAND ----------

# MAGIC %md ####Install the library

# COMMAND ----------

# MAGIC %pip install dbldatagen

# COMMAND ----------

# MAGIC %md ### Generate Employee Data using following format
# MAGIC | EMP_ID| NAME | GENDER|EMAIL|PHONE
# MAGIC | ----------- | ----------- |----------- |----------- |--------------|
# MAGIC | 1 | 'person 1' | 'M'| perso1@ymail.com|1(055) 747-9692
# MAGIC | 1 | 'person 2' | 'F'| perso2@hyth.com|834 8856608

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType

data_rows = 1000000
emp_data_spec = (
    dg.DataGenerator(spark, name="employee_dataset", rows=data_rows)
    .withColumn("EMP_ID", IntegerType(), minValue=1000, maxValue=1000 + data_rows)
    .withColumn("NAME", template=r"\\w \\w|\\w a. \\w")
    .withColumn("EMAIL", template=r"\\w.\\w@\\w.com", nullable=False)
    .withColumn("GENDER", StringType(), values=["M", "F", "O"])
    .withColumn("PHONE",percentNulls=0.05, template=r"(ddd)-ddd-dddd|1(ddd) ddd-dddd|ddd ddddddd")
)

emp_df = emp_data_spec.build()
display(emp_df)

# COMMAND ----------

# MAGIC %md ### Generate Stores Sales Data in below format
# MAGIC 
# MAGIC | STORE_ID| invoice_id | amount|product_code| purchase_date|
# MAGIC | ----------- | ----------- |----------- |----------- |----------- |
# MAGIC | 1234|7263727|177|278399|2022-01-05
# MAGIC | 5258|7263727|177|278399|2022-01-05

# COMMAND ----------

import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType

store_id = range(400, 500)

row_count = 10000000
sales_data_spec = (
    dg.DataGenerator(
        spark,
        name="sales_data",
        rows=row_count,
        partitions=4,
    )
    .withColumn("store_id", IntegerType(), values=store_id)
    .withColumn("invoice_id", IntegerType(), minValue=1000000, maxValue=2000000)
    .withColumn("amount", template=r"\\N.d")
    .withColumn("product_code", template=r"A\N")
    .withColumn("purchase_date", "date", uniqueValues=300)
)

sales_data = sales_data_spec.build()
display(sales_data)

# COMMAND ----------

# MAGIC %md ### Check the number of partitions

# COMMAND ----------

sales_data.rdd.getNumPartitions()
