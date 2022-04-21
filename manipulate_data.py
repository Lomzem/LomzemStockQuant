import requests
import os
import pandas as pd
import datetime

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
df = pd.read_excel('/content/drive/MyDrive/LomzemStockCharts/cumulative_stock_data.xlsx')
ddf = pd.DataFrame()

ddf = pd.concat([ddf, df[['Ticker', 'FTime']]])
ddf['FTime'] = ddf['FTime'].dt.date

# df.loc[192]
# Get Open
t_df = df.loc[df['FTime'].dt.time==datetime.time(9,30), ['Ticker', 'FTime', 'Open']]
t_df['FTime'] = t_df['FTime'].dt.date
ddf = ddf.merge(t_df, on=['Ticker', 'FTime'])

# Get Close
t_df = df.loc[df['FTime'].dt.time==datetime.time(15,59), ['Ticker', 'FTime', 'Close']]
t_df['FTime'] = t_df['FTime'].dt.date
ddf = ddf.merge(t_df, on=['Ticker', 'FTime'])

# Get High
t_df = df.loc[(df['FTime'].dt.time>=datetime.time(9,30)) & (df['FTime'].dt.time<=datetime.time(15,59))]
t_df = t_df[['Ticker', 'FTime', 'High']]
t_df['FTime'] = t_df['FTime'].dt.date
idx = t_df.groupby(['Ticker', 'FTime'])['High'].transform(max) == t_df['High']
t_df = t_df[idx].drop_duplicates()
ddf = ddf.merge(t_df, on=['Ticker', 'FTime'])

# Get Low
t_df = df.loc[(df['FTime'].dt.time>=datetime.time(9,30)) & (df['FTime'].dt.time<=datetime.time(15,59))]
t_df = t_df[['Ticker', 'FTime', 'Low']]
t_df['FTime'] = t_df['FTime'].dt.date
idx = t_df.groupby(['Ticker', 'FTime'])['Low'].transform(min) == t_df['Low']
t_df = t_df[idx].drop_duplicates()
ddf = ddf.merge(t_df, on=['Ticker', 'FTime'])

# Get Volume
t_df = df.loc[(df['FTime'].dt.time>=datetime.time(4,0)) & (df['FTime'].dt.time<=datetime.time(20,0))]
t_df = t_df[['Ticker', 'FTime', 'Volume']]
t_df['FTime'] = t_df['FTime'].dt.date
t_df = t_df.groupby(['Ticker', 'FTime'])['Volume'].sum()
t_df.drop_duplicates(inplace=True)
ddf = ddf.merge(t_df, on=['Ticker', 'FTime'])

# Get Premarket Volume
t_df = df.loc[(df['FTime'].dt.time>=datetime.time(4,0)) & (df['FTime'].dt.time<datetime.time(9,30))]
t_df = t_df[['Ticker', 'FTime', 'Volume']]
t_df['FTime'] = t_df['FTime'].dt.date
t_df.rename(columns={'Volume': 'PVolume'}, inplace=True)
t_df = t_df.groupby(['Ticker', 'FTime'])['PVolume'].sum()
t_df.drop_duplicates(inplace=True)
ddf = ddf.merge(t_df, on=['Ticker', 'FTime'])

# Get Gap %
ddf.sort_values(['Ticker', 'FTime'], inplace=True)
ddf.rename(columns={'FTime': 'Date'}, inplace=True)
ddf.drop_duplicates(inplace=True)
ddf['Gap %'] = (ddf['Open'] / ddf.groupby('Ticker').shift(1)['Close'] - 1) * 100

# Get Volume Ratio
ddf['Vol Ratio'] = ddf['Volume']/ddf['PVolume']

ddf
# print(ddf.head())
# print(ddf.groupby('Ticker')['Close'].shift(1).head())
# t_df