import pandas as pd
from sqlalchemy import create_engine

# Replace placeholders with your actual database connection details
db_username = "tcp0077"
db_password = "TCP0077#482023"
db_host = "taradatadb.csjcqfxduoov.ap-south-1.rds.amazonaws.com"
db_port = "53245"
db_name = "tcp_data"
schema_name = "marketdata"
table_name = "equity_price_history_adjusted_201802"

# Create a database engine
db_engine = create_engine(f'postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Load data into a DataFrame
query = f"SELECT * FROM {schema_name}.{table_name} LIMIT 100;"

try:
    df_top_100 = pd.read_sql(query, db_engine)
except pd.errors.DatabaseError as e:
    print("Error: Unable to fetch data")
    print(e)

# Print the DataFrame
print(df_top_100)
