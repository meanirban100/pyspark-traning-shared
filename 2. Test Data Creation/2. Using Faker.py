# Databricks notebook source
# MAGIC %md ### Test Data generation using faker
# MAGIC 
# MAGIC For more information refer documentation
# MAGIC  - https://faker.readthedocs.io/en/master/
# MAGIC  
# MAGIC GIT
# MAGIC - https://github.com/joke2k/faker

# COMMAND ----------

# MAGIC %md ####Install the library

# COMMAND ----------

!pip install faker

# COMMAND ----------

# MAGIC %md ### Generate Employee Data using following format
# MAGIC | EMP_ID| NAME | GENDER|EMAIL|PHONE
# MAGIC | ----------- | ----------- |----------- |----------- |--------------|
# MAGIC | 1 | 'person 1' | 'M'| perso1@ymail.com|1(055) 747-9692
# MAGIC | 1 | 'person 2' | 'F'| perso2@hyth.com|834 8856608

# COMMAND ----------

from faker import Faker
import numpy as np
import pandas as pd
df = pd.DataFrame()

faker = Faker()

for i in range(1000,2000):

    gender = np.random.choice(["M", "F", "Others"])

    df = df.append(
        {
            "EMP_ID": int(i),
            "NAME": faker.name(),
            "GENDER": gender,
            "EMAIL": faker.email(),
            "PHONE": faker.phone_number(),
        },
        ignore_index=True,
    )

#convert the EMP_ID as Integer
df["EMP_ID"] = df["EMP_ID"].astype(int)

# COMMAND ----------

# MAGIC %md ### Convert the pandas dataframe to spark dataframe

# COMMAND ----------

emp_df = spark.createDataFrame(df)
display(emp_df)

# COMMAND ----------

# MAGIC %md ### Generate Stores Sales Data in below format
# MAGIC 
# MAGIC | STORE_ID| invoice_id | amount|product_code| purchase_date|
# MAGIC | ----------- | ----------- |----------- |----------- |----------- |
# MAGIC | 1234|7263727|177|278399|2022-01-05
# MAGIC | 5258|7263727|177|278399|2022-01-05

# COMMAND ----------

from faker import Faker
import numpy as np
import pandas as pd
df = pd.DataFrame()

faker = Faker()

for i in range(1000,2000):

    gender = np.random.choice(["M", "F", "Others"])

    df = df.append(
        {
            "STORE_ID": int(i),
            "INVOICE_ID": 'INV_'+str(faker.pyint()),
            "AMOUNT": np.random.choice(range(100,1000)),
            "PRODUCT_CODE": faker.pyint(),
            "PURCHASE_DATE": faker.date_between_dates(date_start=datetime(2020,1,1))
        },
        ignore_index=True,
    )

#convert the PRODUCT_CODE as Integer
df["PRODUCT_CODE"] = df["PRODUCT_CODE"].astype(int)

# COMMAND ----------

# MAGIC %md ### Convert the pandas dataframe to spark dataframe

# COMMAND ----------

store_df = spark.createDataFrame(df)
display(store_df)

# COMMAND ----------


