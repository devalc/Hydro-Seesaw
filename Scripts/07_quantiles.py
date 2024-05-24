# -*- coding: utf-8 -*-
"""
Updated on Friday May 24 04:12:19 2024

Purpose: Calculate percentile values at 1,5,10,90,95,99
@author: devalc, Siddharth Chaudhary
"""

import pandas as pd
import pyarrow.parquet as pq

# Read the Parquet file into a DataFrame
df = pd.read_parquet('..//data//Processed//grouped_median_discharge_nonNA.parquet')

# Convert the 'time' column to datetime
df['time'] = pd.to_datetime(df['time'])

# Filter the DataFrame where scenario is 'hist'
filtered_df = df[df['scenario'] == 'hist']

# Define the percentiles to calculate
percentiles = [1, 5, 10, 90, 95, 99]

# Group by 'station_name' and calculate the percentiles
percentile_df = filtered_df.groupby('station_name')['discharge'].quantile([p/100 for p in percentiles]).unstack()

# Rename the columns for clarity
percentile_df.columns = [f'{int(p)}th_percentile' for p in percentiles]

percentile_df.to_csv('..//data//Processed//percentile_results_hist.csv')
