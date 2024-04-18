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
table1_name = "equity_price_history_adjusted_2018"
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
data = pd.merge(df_table1, df_table2, on=common_column)

# Assuming your DataFrame is named 'data' and it has 'date', 'symbol', and 'price' columns

# Function to calculate the entry and exit conditions
def calculate_entry_exit_conditions(price_series, moving_avg, std_dev, observation_days, entry_bandwidth, exit_bandwidth):
    # Calculate entry and exit bandwidths
    entry_upper_bandwidth = moving_avg + entry_bandwidth * std_dev
    entry_lower_bandwidth = moving_avg - entry_bandwidth * std_dev
    exit_upper_bandwidth = moving_avg + exit_bandwidth * std_dev
    exit_lower_bandwidth = moving_avg - exit_bandwidth * std_dev

    # Calculate the closing price 'observation_days' ago
    closing_price_observation = price_series.iloc[-observation_days]

    # Check if the conditions for entry and exit are met
    condition_entry = (closing_price_observation > entry_upper_bandwidth) & (price_series.iloc[-1] < entry_upper_bandwidth)
    condition_exit = (closing_price_observation < entry_lower_bandwidth) & (price_series.iloc[-1] > entry_lower_bandwidth)

    return condition_entry, condition_exit

# Function to trade the pairs
def trade_pairs(pair_list, entry_exit_conditions, data):
    open_positions = {}  # To track open positions

    for idx, row in data.iterrows():
        current_date = row['date']
        current_symbol = row['symbol']
        current_price = row['price']

        # Check if there's an open position for the current symbol
        if current_symbol in open_positions:
            entry_condition, exit_condition = entry_exit_conditions[current_symbol]
            if exit_condition:
                # Implement exit strategy
                if current_price < entry_exit_conditions[current_symbol][1]:
                    print(f"Exiting: {current_symbol} at price {current_price}")
                    open_positions.pop(current_symbol)  # Close the position
                else:
                    print(f"No exit for: {current_symbol} at price {current_price}")
            else:
                print(f"No exit condition for: {current_symbol} at price {current_price}")
        else:
            # Check entry condition for the current symbol
            entry_condition, _ = entry_exit_conditions[current_symbol]
            if entry_condition:
                print(f"Entering: {current_symbol} at price {current_price}")
                open_positions[current_symbol] = current_price  # Open a position

# Step 1: Filter the DataFrame for the specified date range
start_date = '2018-02-22'
end_date = '2018-03-10'  # Adjust the end date as needed
lookup_days = 5
moving_average_days = 20  # Adjust this value as needed
end_date_lookup = pd.to_datetime(start_date) - pd.Timedelta(days=lookup_days)
end_date_moving_avg = pd.to_datetime(start_date) - pd.Timedelta(days=moving_average_days)

# Filter data for both lookup days and moving average window
test_df = data[(data['trade_date'] >= end_date_lookup) & (data['trade_date'] <= pd.to_datetime(start_date))]
#filtered_data_moving_avg = data[(data['trade_date'] >= end_date_moving_avg) & (data['trade_date'] <= pd.to_datetime(start_date))]
testMV_df=data[(data['trade_date'] >= end_date_moving_avg) & (data['trade_date'] <= pd.to_datetime(start_date))]
filtered_data_moving_avg=testMV_df[testMV_df['exchange_ticker'].isin(symbols)]
filtered_data_lookup=test_df[test_df['exchange_ticker'].isin(symbols)]

# Step 2: Pivot the DataFrame for lookup days
pivot_data_lookup = filtered_data_lookup.pivot(index='trade_date', columns='exchange_ticker', values='close')
print(pivot_data_lookup)

# Step 3: Calculate the correlation matrix for lookup days
correlation_matrix = pivot_data_lookup.corr()
print(correlation_matrix)

# Step 4: Find symbol pairs with correlation less than -0.8
pairs_with_low_correlation = []
for symbol1 in correlation_matrix.columns:
    for symbol2 in correlation_matrix.columns:
        if symbol1 != symbol2 and correlation_matrix.loc[symbol1, symbol2] < -0.8:
            pairs_with_low_correlation.append((symbol1, symbol2))
print(pairs_with_low_correlation)
# Step 5: Calculate the entry and exit conditions for symbols in the low correlation pairs
entry_exit_conditions = {}
for symbol in correlation_matrix.columns:
    if any(symbol in pair for pair in pairs_with_low_correlation):
        symbol_data_moving_avg = filtered_data_moving_avg[filtered_data_moving_avg['exchange_ticker'] == symbol]
        print(symbol_data_moving_avg)
        symbol_data_moving_avg = symbol_data_moving_avg.set_index('trade_date')
        print(symbol_data_moving_avg)
        moving_avg = symbol_data_moving_avg['close'].mean()
        std_dev = symbol_data_moving_avg['close'].std()

        print(moving_avg)
        print(std_dev)

        # Calculate entry and exit conditions
        entry_condition, exit_condition = calculate_entry_exit_conditions(
            symbol_data_moving_avg['close'], moving_avg, std_dev,
            observation_days=10, entry_bandwidth=1.5, exit_bandwidth=0.5)

        entry_exit_conditions[symbol] = (entry_condition, exit_condition)

# Step 6: Trade the pairs based on the calculated entry and exit conditions for the specified date range
filtered_data_for_trading = data[(data['trade_date'] >= end_date_lookup) & (data['trade_date'] <= pd.Timedelta(end_date))]
trade_pairs(pairs_with_low_correlation, entry_exit_conditions, filtered_data_for_trading)
