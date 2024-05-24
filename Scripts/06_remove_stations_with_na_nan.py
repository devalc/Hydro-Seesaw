# -*- coding: utf-8 -*-
"""
Updated on Friday May 24 12:15:13 2024

Purpose: check na and list stations with NaNs, remove these stations, and save the cleaned dataset
@author: devalc
"""


import dask.dataframe as dd
import pandas as pd

# Ensure pyarrow is installed
# You can install it using the following command: pip install pyarrow

# Read the list of stations with NaNs from the text file and convert them to integers
stations_with_nans_file = '..//data//stations_with_nans.txt'
with open(stations_with_nans_file, 'r') as f:
    stations_with_nans_list = [int(float(line.strip())) for line in f]

# Convert the list to a set for efficient filtering
stations_with_nans_set = set(stations_with_nans_list)

# Read Parquet file into a Dask DataFrame
ddf = dd.read_parquet('..//data//grouped_median_discharge.parquet')

# Filter out the stations with NaNs from the original Dask DataFrame
filtered_ddf = ddf[~ddf['station_name'].isin(stations_with_nans_set)]

# Repartition the filtered DataFrame to a single partition
filtered_ddf = filtered_ddf.repartition(npartitions=1)

# Write the filtered DataFrame to a new Parquet file using pyarrow
output_parquet_file = '..//data//Processed//grouped_median_discharge_nonNA.parquet'
filtered_ddf.to_parquet(output_parquet_file, write_index=False, engine='pyarrow', compression='snappy')

print(f"Filtered Parquet file '{output_parquet_file}' has been created.")