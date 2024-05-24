# -*- coding: utf-8 -*-
"""
Updated on Friday Apr 17 12:15:13 2024

Purpose: check na and list stations with NaNs
@author: devalc
"""

import dask.dataframe as dd
import pandas as pd

# Read Parquet file into a Dask DataFrame
ddf = dd.read_parquet('..//data//grouped_median_discharge.parquet')

# Initialize an empty list to store intermediate results and stations with NaNs
intermediate_results = []
stations_with_nans = set()

# Define chunk size (adjust as needed)
chunk_size = 100000  # for example

# Iterate over chunks of the DataFrame
for chunk in ddf.to_delayed():
    # Process chunk and compute missing values
    chunk_df = chunk.compute()
    result = chunk_df.groupby(['station_name', 'scenario']).apply(lambda x: x.isna().sum(), include_groups=False)
    # Convert sums to integers
    result = result.astype(int)
    # Append intermediate result to the list
    intermediate_results.append(result)
    
    # Find stations with NaNs in this chunk
    stations_with_nan_in_chunk = chunk_df.loc[chunk_df.isna().any(axis=1), 'station_name'].unique()
    stations_with_nans.update(stations_with_nan_in_chunk)

# Concatenate intermediate results into a single DataFrame
missing_values_grouped = pd.concat(intermediate_results)

# Reset index to ensure 'station_name' is not set as an index
missing_values_grouped.reset_index(inplace=True)

# Aggregate results and convert to integers
missing_values_grouped = missing_values_grouped.groupby(['station_name', 'scenario']).sum().astype(int)

# Write the DataFrame to an Excel file
output_excel_file = '..//data//missing_values_analysis.xlsx'
missing_values_grouped.to_excel(output_excel_file)

# Write the list of stations with NaNs to a text file
stations_with_nans_file = '..//data//stations_with_nans.txt'
with open(stations_with_nans_file, 'w') as f:
    for station in sorted(stations_with_nans):
        f.write(f"{station}\n")

print(f"Output Excel file '{output_excel_file}' has been created.")
print(f"List of stations with NaNs has been written to '{stations_with_nans_file}'.")
