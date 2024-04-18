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
merged_df = pd.merge(df_table1, df_table2, on=common_column)

input_date = pd.to_datetime('2018-03-13')
lookup_days = 5

#end_date = input_date - pd.DateOffset(days=lookup_days)
end_date = input_date
remaining_days = lookup_days

while remaining_days > 0:
    end_date = end_date - pd.DateOffset(days=1)
    if end_date.weekday() < 5:  # Monday to Friday are weekdays (0 to 4)
        remaining_days -= 1
#print(end_date)


filtered_df = merged_df[(merged_df['trade_date'] >= end_date) & (merged_df['trade_date'] <= input_date - pd.DateOffset(days=1))]
#filtered_df = merged_df[merged_df['exchange_ticker'].isin(symbols)]
symbol_df=filtered_df[filtered_df['exchange_ticker'].isin(symbols)]
#symbol_df=filtered_df[filtered_df['exchange_ticker']=='ASHOKLEY']

# Calculate the average of value_column based on distinct values of another_column
#average_by_column = symbol_df.groupby('exchange_ticker')['close'].mean()
#print(average_by_column)

pivot_df = symbol_df.pivot_table(index='trade_date', columns='exchange_ticker', values='close')
print(pivot_df)
correlation_matrix = pivot_df.corr()
print(correlation_matrix)

#selected_correlations = correlation_matrix[correlation_matrix < -0.8]
#print(selected_correlations)

row_names, col_names = np.where(correlation_matrix < -0.7)
#Print the pairs of row and column names
# Write the pairs of row and column names to a text file
with open('final_symbols.txt', 'w') as f1:
    with open('pairs.csv', 'w') as f:
        f.write("symbol1,symbol2\n")
        for row, col in zip(row_names, col_names):
            row_name = correlation_matrix.index[row]
            col_name = correlation_matrix.columns[col]
            correlation_value = correlation_matrix.iloc[row, col]
            print(f"Row: {row_name}, Column: {col_name}, Correlation: {correlation_value}")
            f1.write(f"{row_name}\n")
            f1.write(f"{col_name}\n")
            f.write(f"{row_name},{col_name}\n")

movingAverage_days = 11

with open('final_symbols.txt', 'r') as f:
    finalSymbols = f.read().splitlines()

#end_date_ma = input_date - pd.DateOffset(days=movingAverage_days)
end_date_ma = input_date
remaining_days = movingAverage_days

while remaining_days > 0:
    end_date_ma = end_date_ma - pd.DateOffset(days=1)
    if end_date_ma.weekday() < 5:  # Monday to Friday are weekdays (0 to 4)
        remaining_days -= 1
    #print(end_date_ma, remaining_days)
#print(end_date_ma)

filtered_ma_df = merged_df[(merged_df['trade_date'] >= end_date_ma) & (merged_df['trade_date'] <= input_date - pd.DateOffset(days=1))]
finalSymbol_df=filtered_ma_df[filtered_ma_df['exchange_ticker'].isin(finalSymbols)]
pivot_ma_df = finalSymbol_df.pivot_table(index='trade_date', columns='exchange_ticker', values='close')
print(pivot_ma_df)

average_by_column = finalSymbol_df.groupby('exchange_ticker')['close'].agg(mean_values='mean')
average_by_column=average_by_column.reset_index()
print(average_by_column)

standard_deviation_by_column=finalSymbol_df.groupby('exchange_ticker')['close'].agg(std_dev_value='std')
standard_deviation_by_column=standard_deviation_by_column.reset_index()
print(standard_deviation_by_column)

new_df = pd.DataFrame(columns=['exchange_ticker', 'entry_upper_bandwidth','entry_lower_bandwidth','exit_upper_bandwidth','exit_lower_bandwidth'])

BentryWidth=0.5
BoutWidth=0.5
# Loop through the 'symbol' column of the original DataFramei

for (std_dev_index, std_dev_row), (mean_index, mean_row) in zip(standard_deviation_by_column.iterrows(), average_by_column.iterrows()):
    # Perform calculation using values from both DataFrames
    entry_upper_bandwidth = (std_dev_row['std_dev_value'] * BentryWidth) + mean_row['mean_values']  # Example calculation
    entry_lower_bandwidth = mean_row['mean_values'] - (std_dev_row['std_dev_value'] * BentryWidth) 

    exit_upper_bandwidth = (std_dev_row['std_dev_value'] * BoutWidth) + mean_row['mean_values']
    exit_lower_bandwidth = mean_row['mean_values'] - (std_dev_row['std_dev_value'] * BoutWidth)

    
    # Append the calculated values to the new DataFrame
    new_df =new_df.append({'exchange_ticker': std_dev_row['exchange_ticker'], 'entry_upper_bandwidth': entry_upper_bandwidth,'entry_lower_bandwidth':entry_lower_bandwidth, 'exit_upper_bandwidth':exit_upper_bandwidth, 'exit_lower_bandwidth':exit_lower_bandwidth}, ignore_index=True)

# Print the calculated DataFrame
print(new_df)
#pairs_df=pd.DataFrame(columns=['symbol1', 'symbol2'])

pairs_df=pd.read_csv('pairs.csv')
print(pairs_df)

observation_days = 2
#end_date_od = input_date - pd.DateOffset(days=observation_days)
end_date_od = input_date
remaining_days = observation_days

while remaining_days > 0:
    end_date_od = end_date_od - pd.DateOffset(days=1)
    if end_date_od.weekday() < 5:  # Monday to Friday are weekdays (0 to 4)
        remaining_days -= 1
#print(end_date_od)

filtered_od_df = merged_df[(merged_df['trade_date'] == end_date_od)]
observation_df=filtered_od_df[filtered_od_df['exchange_ticker'].isin(finalSymbols)]
print(observation_df)
filtered_today_df = merged_df[(merged_df['trade_date'] == input_date)]
today_df=filtered_today_df[filtered_today_df['exchange_ticker'].isin(finalSymbols)]

with open('traded_symbols.txt', 'w') as fsymb:
    with open('traded_pairs.csv', 'w') as fpair:
        fpair.write("symbol1,symbol2,price1,price2\n")
        for index, row in pairs_df.iterrows():
            symbol1=row['symbol1']
            symbol2=row['symbol2']
            val1=int(new_df.loc[new_df['exchange_ticker'] == symbol1,'entry_upper_bandwidth'])
            val2=int(new_df.loc[new_df['exchange_ticker'] == symbol2,'entry_lower_bandwidth'])
            #print(val1,val2)
            cpOd1=int(observation_df.loc[observation_df['exchange_ticker']==symbol1,'close'])
            cpOd2=int(observation_df.loc[observation_df['exchange_ticker']==symbol2,'close'])
            cpT1=int(today_df.loc[today_df['exchange_ticker']==symbol1,'close'])
            cpT2=int(today_df.loc[today_df['exchange_ticker']==symbol2,'close'])
            if ((cpOd1 > val1) and (val1 > cpT1)):
                if ((cpOd2 < val2) and (val2 < cpT2)):
                    fpair.write(f"{symbol1},{symbol2},{cpT1},{cpT2}\n")
                    fsymb.write(f"{symbol1}\n")
                    fsymb.write(f"{symbol2}\n")
                    print("trade")
        #print(cpOd1,val1,cpT1,cpOd2,val2,cpT2)


#to handle the traded pairs to exit the position

with open('traded_symbols_last.txt', 'r') as f:
    finalSymbols = f.read().splitlines()

#end_date_ma = input_date - pd.DateOffset(days=movingAverage_days)
end_date_ma = input_date
remaining_days = movingAverage_days

while remaining_days > 0:
    end_date_ma = end_date_ma - pd.DateOffset(days=1)
    if end_date_ma.weekday() < 5:  # Monday to Friday are weekdays (0 to 4)
        remaining_days -= 1
    #print(end_date_ma, remaining_days)
#print(end_date_ma)

filtered_ma_df = merged_df[(merged_df['trade_date'] >= end_date_ma) & (merged_df['trade_date'] <= input_date - pd.DateOffset(days=1))]
finalSymbol_df=filtered_ma_df[filtered_ma_df['exchange_ticker'].isin(finalSymbols)]
pivot_ma_df = finalSymbol_df.pivot_table(index='trade_date', columns='exchange_ticker', values='close')
print(pivot_ma_df)

average_by_column = finalSymbol_df.groupby('exchange_ticker')['close'].agg(mean_values='mean')
average_by_column=average_by_column.reset_index()
print(average_by_column)

standard_deviation_by_column=finalSymbol_df.groupby('exchange_ticker')['close'].agg(std_dev_value='std')
standard_deviation_by_column=standard_deviation_by_column.reset_index()
print(standard_deviation_by_column)

new_df = pd.DataFrame(columns=['exchange_ticker', 'entry_upper_bandwidth','entry_lower_bandwidth','exit_upper_bandwidth','exit_lower_bandwidth'])

BentryWidth=0.5
BoutWidth=0.5
# Loop through the 'symbol' column of the original DataFramei

for (std_dev_index, std_dev_row), (mean_index, mean_row) in zip(standard_deviation_by_column.iterrows(), average_by_column.iterrows()):
    # Perform calculation using values from both DataFrames
    entry_upper_bandwidth = (std_dev_row['std_dev_value'] * BentryWidth) + mean_row['mean_values']  # Example calculation
    entry_lower_bandwidth = mean_row['mean_values'] - (std_dev_row['std_dev_value'] * BentryWidth)

    exit_upper_bandwidth = (std_dev_row['std_dev_value'] * BoutWidth) + mean_row['mean_values']
    exit_lower_bandwidth = mean_row['mean_values'] - (std_dev_row['std_dev_value'] * BoutWidth)


    # Append the calculated values to the new DataFrame
    new_df =new_df.append({'exchange_ticker': std_dev_row['exchange_ticker'], 'entry_upper_bandwidth': entry_upper_bandwidth,'entry_lower_bandwidth':entry_lower_bandwidth, 'exit_upper_bandwidth':exit_upper_bandwidth, 'exit_lower_bandwidth':exit_lower_bandwidth}, ignore_index=True)

# Print the calculated DataFrame
print(new_df)

tradedPairs_df=pd.read_csv('traded_pairs_last.csv')
print(tradedPairs_df)

observation_days = 1
#end_date_od = inpuit_date - pd.DateOffset(days=observation_days)
#end_date_od = input_date - pd.DateOffset(days=observation_days)
end_date_od = input_date
remaining_days = observation_days

while remaining_days > 0:
    end_date_od = end_date_od - pd.DateOffset(days=1)
    if end_date_od.weekday() < 5:  # Monday to Friday are weekdays (0 to 4)
        remaining_days -= 1
#print(end_date_od)


filtered_od_df = merged_df[(merged_df['trade_date'] == end_date_od)]
observation_df=filtered_od_df[filtered_od_df['exchange_ticker'].isin(finalSymbols)]
filtered_today_df = merged_df[(merged_df['trade_date'] == input_date)]
today_df=filtered_today_df[filtered_today_df['exchange_ticker'].isin(finalSymbols)]

with open('trade_completed.csv', 'w') as ftrades:
    ftrades.write("symbol1,symbol2,exitprice1,exitprice2\n")
    for index, row in tradedPairs_df.iterrows():
        symbol1=row['symbol1']
        symbol2=row['symbol2']
        val1=int(new_df.loc[new_df['exchange_ticker'] == symbol1,'exit_lower_bandwidth'])
        val2=int(new_df.loc[new_df['exchange_ticker'] == symbol2,'exit_upper_bandwidth'])
        #print(val1,val2)
        cpOd1=int(observation_df.loc[observation_df['exchange_ticker']==symbol1,'close'])
        cpOd2=int(observation_df.loc[observation_df['exchange_ticker']==symbol2,'close'])
        cpT1=int(today_df.loc[today_df['exchange_ticker']==symbol1,'close'])
        cpT2=int(today_df.loc[today_df['exchange_ticker']==symbol2,'close'])
        if ((cpOd1 > val1) and (val1 > cpT1)):
            if ((cpOd2 < val2) and (val2 < cpT2)):
                ftrades.write(f"{symbol1},{symbol2},{cpT1},{cpT2}\n")
                print("trade")
        print(cpOd1,val1,cpT1,cpOd2,val2,cpT2)



    


#for index, row in standard_deviation_by_column.iterrows():
    # Perform calculation using the std_dev_value
    #upper_bandwidth = row['std_dev_value'] * BentryWidth + average_by_column.loc[index]['mean_values']  # Example calculation

    # Append the calculated values to the new DataFrame
    #new_df = new_df.append({'exchange_ticker': row['exchange_ticker'], 'upper_bandwidth': upper_bandwidth}, ignore_index=True)

# Print the calculated DataFrame
#print(new_df)
# Print the merged DataFrame
#print(symbol_df)
