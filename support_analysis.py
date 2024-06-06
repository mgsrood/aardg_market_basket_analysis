import os
from google.cloud import bigquery
import pandas as pd
from dotenv import load_dotenv
from itertools import permutations
from mlxtend.preprocessing import TransactionEncoder
from google.auth import load_credentials_from_file
from google.oauth2 import service_account
import pandas_gbq as pd_gbq
import json

# Load variables from .env file
load_dotenv()

# Get the GCP keys
gc_keys = os.getenv("AARDG_GOOGLE_CREDENTIALS")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = gc_keys

credentials = service_account.Credentials.from_service_account_file(gc_keys)
project_id = credentials.project_id
client = bigquery.Client(credentials=credentials, project=project_id)

# Define the SQL query for all data
order_sql_query = f"""
SELECT
  order_id,
  date_created,
  total,
  STRING_AGG(base_product, ', ') AS base_product_array
FROM
  `aardg-data.order_data.unnested_order_data`
GROUP BY order_id, date_created, total
"""

# Execute the query and fetch the result
order_query_job = client.query(order_sql_query)
order_results = order_query_job.result()

# Extract the column names from the schema
order_column_names = [field.name for field in order_results.schema]

# Create a Pandas DataFrame from the query result
df = pd.DataFrame(data=[list(row.values()) for row in order_results], columns=order_column_names)

# Turn base_product column into an array
df['base_product_array'] = df['base_product_array'].apply(lambda x: x.split(', ') if isinstance(x, str) else [])

# Turn base product column to a list of lists
base_product_list = df['base_product_array'].tolist()

# Instantiate transaction encoder and identify unique items in transactions
encoder = TransactionEncoder().fit(base_product_list)

# One-hot encode transactions
onehot = encoder.transform(base_product_list)

# Convert one-hot encoded data to DataFrame
onehot = pd.DataFrame(onehot, columns = encoder.columns_)

# Compute the support
support = onehot.mean()

# Turn the support data into a DataFrame
support_df_all = support.reset_index()
support_df_all.columns = ['product', 'support']

# Define the SQL query for the last two years
order_sql_query = f"""
SELECT
  order_id,
  date_created,
  total,
  STRING_AGG(base_product, ', ') AS base_product_array
FROM
  `aardg-data.order_data.unnested_order_data`
WHERE
  date_created >= DATE_SUB(CURRENT_DATE(), INTERVAL 730 DAY)
GROUP BY
  order_id, date_created, total
"""

# Execute the query and fetch the result
order_query_job = client.query(order_sql_query)
order_results = order_query_job.result()

# Extract the column names from the schema
order_column_names = [field.name for field in order_results.schema]

# Create a Pandas DataFrame from the query result
df = pd.DataFrame(data=[list(row.values()) for row in order_results], columns=order_column_names)

# Turn base_product column into an array
df['base_product_array'] = df['base_product_array'].apply(lambda x: x.split(', ') if isinstance(x, str) else [])

# Turn base product column to a list of lists
base_product_list = df['base_product_array'].tolist()

# Instantiate transaction encoder and identify unique items in transactions
encoder = TransactionEncoder().fit(base_product_list)

# One-hot encode transactions
onehot = encoder.transform(base_product_list)

# Convert one-hot encoded data to DataFrame
onehot = pd.DataFrame(onehot, columns = encoder.columns_)

# Compute the support
support = onehot.mean()

# Turn the support data into a DataFrame
support_df_730 = support.reset_index()
support_df_730.columns = ['product', 'support']

# Define the SQL query for the last year
order_sql_query = f"""
SELECT
  order_id,
  date_created,
  total,
  STRING_AGG(base_product, ', ') AS base_product_array
FROM
  `aardg-data.order_data.unnested_order_data`
WHERE
  date_created >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
GROUP BY
  order_id, date_created, total
"""

# Execute the query and fetch the result
order_query_job = client.query(order_sql_query)
order_results = order_query_job.result()

# Extract the column names from the schema
order_column_names = [field.name for field in order_results.schema]

# Create a Pandas DataFrame from the query result
df = pd.DataFrame(data=[list(row.values()) for row in order_results], columns=order_column_names)

# Turn base_product column into an array
df['base_product_array'] = df['base_product_array'].apply(lambda x: x.split(', ') if isinstance(x, str) else [])

# Turn base product column to a list of lists
base_product_list = df['base_product_array'].tolist()

# Instantiate transaction encoder and identify unique items in transactions
encoder = TransactionEncoder().fit(base_product_list)

# One-hot encode transactions
onehot = encoder.transform(base_product_list)

# Convert one-hot encoded data to DataFrame
onehot = pd.DataFrame(onehot, columns = encoder.columns_)

# Compute the support
support = onehot.mean()

# Turn the support data into a DataFrame
support_df_365 = support.reset_index()
support_df_365.columns = ['product', 'support']

# Define the SQL query for the last 180 days
order_sql_query = f"""
SELECT
  order_id,
  date_created,
  total,
  STRING_AGG(base_product, ', ') AS base_product_array
FROM
  `aardg-data.order_data.unnested_order_data`
WHERE
  date_created >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
GROUP BY
  order_id, date_created, total
"""

# Execute the query and fetch the result
order_query_job = client.query(order_sql_query)
order_results = order_query_job.result()

# Extract the column names from the schema
order_column_names = [field.name for field in order_results.schema]

# Create a Pandas DataFrame from the query result
df = pd.DataFrame(data=[list(row.values()) for row in order_results], columns=order_column_names)

# Turn base_product column into an array
df['base_product_array'] = df['base_product_array'].apply(lambda x: x.split(', ') if isinstance(x, str) else [])

# Turn base product column to a list of lists
base_product_list = df['base_product_array'].tolist()

# Instantiate transaction encoder and identify unique items in transactions
encoder = TransactionEncoder().fit(base_product_list)

# One-hot encode transactions
onehot = encoder.transform(base_product_list)

# Convert one-hot encoded data to DataFrame
onehot = pd.DataFrame(onehot, columns = encoder.columns_)

# Compute the support
support = onehot.mean()

# Turn the support data into a DataFrame
support_df_180 = support.reset_index()
support_df_180.columns = ['product', 'support']

# Define the SQL query for the last 90 days
order_sql_query = f"""
SELECT
  order_id,
  date_created,
  total,
  STRING_AGG(base_product, ', ') AS base_product_array
FROM
  `aardg-data.order_data.unnested_order_data`
WHERE
  date_created >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY
  order_id, date_created, total
"""

# Execute the query and fetch the result
order_query_job = client.query(order_sql_query)
order_results = order_query_job.result()

# Extract the column names from the schema
order_column_names = [field.name for field in order_results.schema]

# Create a Pandas DataFrame from the query result
df = pd.DataFrame(data=[list(row.values()) for row in order_results], columns=order_column_names)

# Turn base_product column into an array
df['base_product_array'] = df['base_product_array'].apply(lambda x: x.split(', ') if isinstance(x, str) else [])

# Turn base product column to a list of lists
base_product_list = df['base_product_array'].tolist()

# Instantiate transaction encoder and identify unique items in transactions
encoder = TransactionEncoder().fit(base_product_list)

# One-hot encode transactions
onehot = encoder.transform(base_product_list)

# Convert one-hot encoded data to DataFrame
onehot = pd.DataFrame(onehot, columns = encoder.columns_)

# Compute the support
support = onehot.mean()

# Turn the support data into a DataFrame
support_df_90 = support.reset_index()
support_df_90.columns = ['product', 'support']

# Define the SQL query for the 30 days
order_sql_query = f"""
SELECT
  order_id,
  date_created,
  total,
  STRING_AGG(base_product, ', ') AS base_product_array
FROM
  `aardg-data.order_data.unnested_order_data`
WHERE
  date_created >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY
  order_id, date_created, total
"""

# Execute the query and fetch the result
order_query_job = client.query(order_sql_query)
order_results = order_query_job.result()

# Extract the column names from the schema
order_column_names = [field.name for field in order_results.schema]

# Create a Pandas DataFrame from the query result
df = pd.DataFrame(data=[list(row.values()) for row in order_results], columns=order_column_names)

# Turn base_product column into an array
df['base_product_array'] = df['base_product_array'].apply(lambda x: x.split(', ') if isinstance(x, str) else [])

# Turn base product column to a list of lists
base_product_list = df['base_product_array'].tolist()

# Instantiate transaction encoder and identify unique items in transactions
encoder = TransactionEncoder().fit(base_product_list)

# One-hot encode transactions
onehot = encoder.transform(base_product_list)

# Convert one-hot encoded data to DataFrame
onehot = pd.DataFrame(onehot, columns = encoder.columns_)

# Compute the support
support = onehot.mean()

# Turn the support data into a DataFrame
support_df_30 = support.reset_index()
support_df_30.columns = ['product', 'support']

# Merge the DataFrames to create the support df
support_df = support_df_all.merge(support_df_30, on='product', how='left', suffixes=('', '_30'))
support_df = support_df.merge(support_df_90, on='product', how='left', suffixes=('', '_90'))
support_df = support_df.merge(support_df_180, on='product', how='left', suffixes=('', '_180'))
support_df = support_df.merge(support_df_365, on='product', how='left', suffixes=('', '_365'))
support_df = support_df.merge(support_df_730, on='product', how='left', suffixes=('', '_730'))
support_df = support_df.rename(columns={'support': 'support_all'})

# BigQuery information
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
table_id = os.getenv("SUPPORT_TABLE_ID")

try:
    df_gbq = pd_gbq.to_gbq(support_df, destination_table=f'{project_id}.{dataset_id}.{table_id}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')
    print("Upload naar BigQuery succesvol voltooid!")
except Exception as e:
    error_message = f"Fout bij het uploaden naar BigQuery: {str(e)}"