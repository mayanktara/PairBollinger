import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import sys

arg=sys.argv[1]
print(arg)

data_df=pd.read_csv('../data/finaldata2014.csv')
print(data_df)
holiday_df=pd.read_csv('../data/holiday2014.csv')
print(holiday_df)

input_date = pd.to_datetime(arg)
lookup_days = 20
inpdate=input_date.strftime("%Y-%m-%d")
print(inpdate)
#print((holiday_df['Date']==inpdate).any())
if (((holiday_df['Date']==inpdate).any()==True) or (input_date.weekday()>4)):
    exit(1)


end_date = input_date
remaining_days = lookup_days

while remaining_days > 0:
    end_date = end_date - pd.DateOffset(days=1)
    if (end_date.weekday() < 5) and ((holiday_df['Date']==end_date.strftime("%Y-%m-%d")).any()==False):  # Monday to Friday are weekdays (0 to 4)
        remaining_days -= 1
    print(end_date, remaining_days)


filtered_df = data_df[(pd.to_datetime(data_df['trade_date']) >= end_date) & (pd.to_datetime(data_df['trade_date']) <= input_date - pd.DateOffset(days=1))]

# Calculate the average of value_column based on distinct values of another_column
#average_by_column = symbol_df.groupby('exchange_ticker')['close'].mean()
#print(average_by_column)

pivot_df = filtered_df.pivot_table(index='trade_date', columns='exchange_ticker', values='close')
print(pivot_df)
correlation_matrix = pivot_df.corr()
print(correlation_matrix)

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


movingAverage_days = 20

with open('final_symbols.txt', 'r') as f:
    finalSymbols = f.read().splitlines()

#end_date_ma = input_date - pd.DateOffset(days=movingAverage_days)
end_date_ma = input_date
remaining_days = movingAverage_days

while remaining_days > 0:
    end_date_ma = end_date_ma - pd.DateOffset(days=1)
    if (end_date_ma.weekday() < 5) and ((holiday_df['Date']==end_date_ma.strftime("%Y-%m-%d")).any()==False):  # Monday to Friday are weekdays (0 to 4)
        remaining_days -= 1
    print(end_date_ma, remaining_days)
print(end_date_ma)

filtered_ma_df = data_df[(pd.to_datetime(data_df['trade_date']) >= end_date_ma) & (pd.to_datetime(data_df['trade_date']) <= input_date - pd.DateOffset(days=1))]
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

BentryWidth=1
BoutWidth=0.5

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
    if end_date_od.weekday() < 5 and ((holiday_df['Date']==end_date_od.strftime("%Y-%m-%d")).any()==False):  # Monday to Friday are weekdays (0 to 4)
        remaining_days -= 1
#print(end_date_od)

filtered_od_df = data_df[(pd.to_datetime(data_df['trade_date']) == end_date_od)]
observation_df=filtered_od_df[filtered_od_df['exchange_ticker'].isin(finalSymbols)]
print(observation_df)
filtered_today_df = data_df[(pd.to_datetime(data_df['trade_date']) == input_date)]
today_df=filtered_today_df[filtered_today_df['exchange_ticker'].isin(finalSymbols)]
print(today_df)

tradedPairs_df=pd.read_csv('traded_pairs_last.csv')


with open('traded_symbols.txt', 'w') as fsymb:
    with open('traded_pairs.csv', 'w') as fpair:
        fpair.write("symbol1,symbol2,price1,price2,entrydate,quantity1,quantity2,exitprice1,exitprice2\n")
        for index, row in pairs_df.iterrows():
            symbol1=row['symbol1']
            symbol2=row['symbol2']
            if ((tradedPairs_df['symbol1']==symbol1) & (tradedPairs_df['symbol2']==symbol2)).any()==False:
                val1=int(new_df.loc[new_df['exchange_ticker'] == symbol1,'entry_upper_bandwidth'])
                val2=int(new_df.loc[new_df['exchange_ticker'] == symbol2,'entry_lower_bandwidth'])
                #print(val1,val2)
                exit1=int(new_df.loc[new_df['exchange_ticker'] == symbol1,'exit_lower_bandwidth'])
                exit2=int(new_df.loc[new_df['exchange_ticker'] == symbol2,'exit_upper_bandwidth'])
                cpOd1=int(observation_df.loc[observation_df['exchange_ticker']==symbol1,'close'])
                cpOd2=int(observation_df.loc[observation_df['exchange_ticker']==symbol2,'close'])
                cpT1=int(today_df.loc[today_df['exchange_ticker']==symbol1,'close'])
                cpT2=int(today_df.loc[today_df['exchange_ticker']==symbol2,'close'])
                if ((cpOd1 > val1) and (val1 > cpT1)):
                    if ((cpOd2 < val2) and (val2 < cpT2)):
                        v1 = cpT1
                        v2 = cpT2
                        diff_fact = abs(v1 - v2) / (v1 + v2)
                        min_1 = min_2 = 1
                        min_diff_fact = diff_fact
                        if diff_fact >= 0.2:
                            for i in range(1, 300):
                                for j in range(1, 300):
                                    diff_fact = abs(i * v1 - j * v2) / (i * v1 + j * v2)
                                    if (diff_fact < min_diff_fact):
                                        # if (diff_fact < min_diff_fact):
                                        min_diff_fact = diff_fact
                                        min_1 = i
                                        min_2 = j
                        ttPrice=(v1*min_1)+(v2*min_2);
                        multiple=(1000000/ttPrice);
                        min_1=min_1*multiple;
                        min_2=min_2*multiple;
                        fpair.write(f"{symbol1},{symbol2},{cpT1},{cpT2},{input_date},{min_1},{min_2},{exit1},{exit2}\n")
                        fsymb.write(f"{symbol1}\n")
                        fsymb.write(f"{symbol2}\n")
                        print("trade")
        #print(cpOd1,val1,cpT1,cpOd2,val2,cpT2)


#to handle exit

with open('traded_symbols_last.txt', 'r') as f:
    finalSymbols = f.read().splitlines()

#end_date_ma = input_date - pd.DateOffset(days=movingAverage_days)
end_date_ma = input_date
remaining_days = movingAverage_days

while remaining_days > 0:
    end_date_ma = end_date_ma - pd.DateOffset(days=1)
    if end_date_ma.weekday() < 5 and ((holiday_df['Date']==end_date_ma.strftime("%Y-%m-%d")).any()==False):  # Monday to Friday are weekdays (0 to 4)
        remaining_days -= 1
    #print(end_date_ma, remaining_days)
#print(end_date_ma)

filtered_ma_df = data_df[(pd.to_datetime(data_df['trade_date']) >= end_date_ma) & (pd.to_datetime(data_df['trade_date']) <= input_date - pd.DateOffset(days=1))]
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

BentryWidth=1
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

observation_days = 2
#end_date_od = inpuit_date - pd.DateOffset(days=observation_days)
#end_date_od = input_date - pd.DateOffset(days=observation_days)
end_date_od = input_date
remaining_days = observation_days

while remaining_days > 0:
    end_date_od = end_date_od - pd.DateOffset(days=1)
    if end_date_od.weekday() < 5 and ((holiday_df['Date']==end_date_od.strftime("%Y-%m-%d")).any()==False):  # Monday to Friday are weekdays (0 to 4)
        remaining_days -= 1
#print(end_date_od)


filtered_od_df = data_df[(pd.to_datetime(data_df['trade_date']) == end_date_od)]
observation_df=filtered_od_df[filtered_od_df['exchange_ticker'].isin(finalSymbols)]
filtered_today_df = data_df[(pd.to_datetime(data_df['trade_date']) == input_date)]
today_df=filtered_today_df[filtered_today_df['exchange_ticker'].isin(finalSymbols)]

with open('trade_completed.csv', 'a') as ftrades:
    #ftrades.write("symbol1,symbol2,exitprice1,exitprice2,entryprice1,entryprice2,entrydate,exitdate\n")
    for index, row in tradedPairs_df.iterrows():
        symbol1=row['symbol1']
        symbol2=row['symbol2']
        price1=int(row['price1'])
        price2=int(row['price2'])
        entrydate=row['entrydate']
        quantity1=int(row['quantity1'])
        quantity2=int(row['quantity2'])
        exitcheck1=int(row['exitprice1'])
        exitcheck2=int(row['exitprice2'])
        val1=int(new_df.loc[new_df['exchange_ticker'] == symbol1,'exit_lower_bandwidth'])
        val2=int(new_df.loc[new_df['exchange_ticker'] == symbol2,'exit_upper_bandwidth'])
        #print(val1,val2)
        cpOd1=int(observation_df.loc[observation_df['exchange_ticker']==symbol1,'close'])
        cpOd2=int(observation_df.loc[observation_df['exchange_ticker']==symbol2,'close'])
        cpT1=int(today_df.loc[today_df['exchange_ticker']==symbol1,'close'])
        cpT2=int(today_df.loc[today_df['exchange_ticker']==symbol2,'close'])
        netShort=(price1-cpT1)*quantity1
        netLong=(cpT2-price2)*quantity2
        pnl=netShort+netLong
        totalMon=(((price1*quantity1)+(price2*quantity2))*2)/100
        """
        if ((cpOd1 > val1) and (val1 > cpT1)):
            if ((cpOd2 < val2) and (val2 < cpT2)):
                ftrades.write(f"{symbol1},{symbol2},{cpT1},{cpT2},{price1},{price2},{entrydate},{input_date},{quantity1},{quantity2}\n")
                print("trade")
            else :
                with open('traded_pairs.csv', 'a') as fpair:
                    fpair.write(f"{symbol1},{symbol2},{price1},{price2},{entrydate},{quantity1},{quantity2}\n")
                with open('traded_symbols.txt', 'a') as fsymb:
                    fsymb.write(f"{symbol1}\n")
                    fsymb.write(f"{symbol2}\n")
        else :
            with open('traded_pairs.csv', 'a') as fpair:
                    fpair.write(f"{symbol1},{symbol2},{price1},{price2},{entrydate},{quantity1},{quantity2}\n")
            with open('traded_symbols.txt', 'a') as fsymb:
                    fsymb.write(f"{symbol1}\n")
                    fsymb.write(f"{symbol2}\n")

        print(cpOd1,val1,cpT1,cpOd2,val2,cpT2)
        """
        if ((cpOd1 > exitcheck1) and (exitcheck1 > cpT1)):
            ftrades.write(f"{symbol1},{symbol2},{cpT1},{cpT2},{price1},{price2},{entrydate},{input_date},{quantity1},{quantity2}\n")
            print("trade")
        elif ((cpOd2 < exitcheck2) and (exitcheck2 < cpT2)):
            ftrades.write(f"{symbol1},{symbol2},{cpT1},{cpT2},{price1},{price2},{entrydate},{input_date},{quantity1},{quantity2}\n")
            print("trade")
        else:
            with open('traded_pairs.csv', 'a') as fpair:
                fpair.write(f"{symbol1},{symbol2},{price1},{price2},{entrydate},{quantity1},{quantity2},{exitcheck1},{exitcheck2}\n")
            with open('traded_symbols.txt', 'a') as fsymb:
                fsymb.write(f"{symbol1}\n")
                fsymb.write(f"{symbol2}\n")
        """
        elif (pnl < 0) and (abs(pnl)>totalMon) :
            ftrades.write(f"{symbol1},{symbol2},{cpT1},{cpT2},{price1},{price2},{entrydate},{input_date},{quantity1},{quantity2}\n")
             print("trade")
        else :
            startingDate = pd.to_datetime(entrydate)
            endingDate = pd.to_datetime(input_date)
            ttdays=0
            #print(startingDate)
            #print(endingDate)

            while startingDate!=endingDate:
                #print("in while loop")
                startingDate = startingDate + pd.DateOffset(days=1)
                #print(startingDate)
                if startingDate.weekday() < 5 and ((holiday_df['Date']==startingDate.strftime("%Y-%m-%d")).any()==False):  # Monday to Friday are weekdays (0 to 4)
                    ttdays += 1
                    #print(ttdays)
            if ttdays>12:
                ftrades.write(f"{symbol1},{symbol2},{cpT1},{cpT2},{price1},{price2},{entrydate},{input_date},{quantity1},{quantity2}\n")
                print("trade")
            else:
                with open('traded_pairs.csv', 'a') as fpair:
                    fpair.write(f"{symbol1},{symbol2},{price1},{price2},{entrydate},{quantity1},{quantity2}\n")
                with open('traded_symbols.txt', 'a') as fsymb:
                    fsymb.write(f"{symbol1}\n")
                    fsymb.write(f"{symbol2}\n")
       """
