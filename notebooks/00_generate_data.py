# Databricks notebook source
from faker import Faker
import random
import pandas as pd
import os

# COMMAND ----------

user_email = spark.sql("SELECT current_user() as user").first().asDict().get('user')

# COMMAND ----------

DIRECTORY = f'/dbfs/Users/{user_email}/kmpg/gen_data/'

# COMMAND ----------

DIRECTORY

# COMMAND ----------

def random_case(word:str):
  return ''.join(random.choice((str.upper, str.lower))(char) for char in word)

def append_random_int(word:str, max_value:int=10):
  return 'custom_system_id' + str(random.randint(0,10))

# COMMAND ----------

# DBTITLE 1,Company Data Columns
SYSTEM_ID_COL_NAME = 'system_id'
ADDRESS_1_COL_NAME = 'company_address_1'
ADDRESS_POSTCODE_ZIPCODE_COL_NAME = 'zip_postcode'
COUNTRY = 'country'
COMPANY_NAME = 'company_name'

fake = Faker()
fake_uk = Faker('en_GB')

# COMMAND ----------

# DBTITLE 1,File Gen

custom_columns = ["_".join(fake.words(nb=2)) for _ in range(random.randint(0,4))]

number_of_rows = random.randint(1,1000)

data = {
  random_case(SYSTEM_ID_COL_NAME):[fake.uuid4() for _ in range(number_of_rows)],
  COMPANY_NAME : [fake.company() for _ in range(number_of_rows)],
  random_case(ADDRESS_1_COL_NAME):[fake.street_address() for _ in range(number_of_rows)],
  random_case(ADDRESS_POSTCODE_ZIPCODE_COL_NAME):[random.choice([fake_uk.postcode(), fake.zipcode()]) for _ in range(number_of_rows)],
  COUNTRY : [fake.country() for _ in range(number_of_rows)]                    
}

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install fsspec

# COMMAND ----------

if not os.path.exists(DIRECTORY):
    os.makedirs(DIRECTORY)

file_name = f'company_file_{fake.uuid4()}'
file_path = f'{DIRECTORY}{file_name}'
pd.DataFrame(data).to_csv(file_path, header=True)
