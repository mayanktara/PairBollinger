import pandas as pd
from sqlalchemy import create_engine
import numpy as np

# Replace placeholders with your actual database connection details
db_username = "tcp0077"
db_password = "TCP0077#482023"
db_host = "taradatadb.csjcqfxduoov.ap-south-1.rds.amazonaws.com"
db_port = "53245"
db_name = "tcp_data"
schema_name = "marketdata"
table1_name = "equity_price_history_adjusted_201512"
table2_name = "securities_master"
common_column = "tcp_id"

columns_table1 = ["trade_date", "tcp_id", "close", "equity_series_code"]
columns_table2 = ["tcp_id", "exchange_ticker"]


with open('symbol_file.txt', 'r') as f:
    symbols = f.read().splitlines()
# Create a database engine
db_engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load data from both tables into DataFrames
query1 = f"SELECT {', '.join(columns_table1)} FROM {schema_name}.{table1_name};"
query2 = f"SELECT {', '.join(columns_table2)} FROM {schema_name}.{table2_name};"

try:
    df_table1 = pd.read_sql(query1, db_engine)
    df_table2 = pd.read_sql(query2, db_engine)
except pd.errors.DatabaseError as e:
    print("Error: Unable to fetch data")
    print(e)

# Perform the join using pandas
merged_df = pd.merge(df_table1, df_table2, on=common_column)
symbol_df=merged_df[merged_df['exchange_ticker'].isin(symbols)]
final_df=symbol_df[symbol_df['equity_series_code']=='EQ']
print(final_df)
final_df.to_csv('data.csv')
