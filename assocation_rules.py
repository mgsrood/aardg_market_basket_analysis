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
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
import re

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

# Compute frequent itemsets using the Apriori algorithm
frequent_itemsets = apriori(onehot, min_support = 0.001, 
                            max_len = 3, use_colnames = True)

# Compute all association rules using confidence
rules = association_rules(frequent_itemsets, 
                            metric = "support", 
                         	min_threshold = 0.00)

# Rename columns and change types to fit
rules.rename(columns={'antecedent support': 'antecedent_support'}, inplace=True)
rules.rename(columns={'consequent support': 'consequent_support'}, inplace=True)
rules['antecedents'] = rules['antecedents'].astype(str)
rules['consequents'] = rules['consequents'].astype(str)

# Remove frozenset information
def extract_content(frozenset_string):
    match = re.match(r"frozenset\({(.+?)}\)", frozenset_string)
    if match:
        content = match.group(1)
        return content.strip("'")
    else:
        return None
    
rules['antecedents'] = rules['antecedents'].apply(extract_content)
rules['consequents'] = rules['consequents'].apply(extract_content)

print(rules.head())

# BigQuery information
project_id = os.getenv("PROJECT_ID")
dataset_id = os.getenv("DATASET_ID")
table_id = os.getenv("ASSOCIATION_TABLE_ID")

try:
    df_gbq = pd_gbq.to_gbq(rules, destination_table=f'{project_id}.{dataset_id}.{table_id}', project_id=f'{project_id}', credentials=credentials, if_exists='replace')
    print("Upload naar BigQuery succesvol voltooid!")
except Exception as e:
    print("Er is een fout opgetreden tijdens het uploaden naar BigQuery:", str(e))