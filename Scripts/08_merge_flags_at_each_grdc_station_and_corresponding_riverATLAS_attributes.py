#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script to merge a CSV file and a GeoJSON file by the common columns.

This script reads:
1. A CSV file containing latitude, longitude, and flag counts.
2. A GeoJSON file with GRDC station information and attributes.

The script merges these files on the common columns and outputs the result as a new GeoJSON file.

Requirements:
- pandas
- geopandas

Usage:
    python 08_merge_flags_at_each_grdc_station_and_corresponding_riverATLAS_attributes.py

Author: devalc, Siddharth Chaudhary
Date: 07/02/2024
"""
import pandas as pd
import geopandas as gpd
import time

# Start timing the script
start_time = time.time()

# File paths
csv_file = './lat_lon_flag_counts_early_mid_end.csv'
geojson_file = './grdc_stations_with_SWORD_reach_id_and_RiverATLAS_attr_merged.geojson'
output_file = './merged_flags_and_grdc_with_riverATLAS.parquet'  

# Read the CSV file
csv_df = pd.read_csv(csv_file, usecols=lambda column: column not in [0])

# Read the GeoJSON file
geojson_gdf = gpd.read_file(geojson_file)

# Drop the 'geometry' column if present
if 'geometry' in geojson_gdf.columns:
    geojson_gdf = geojson_gdf.drop(columns=['geometry'])

# Rename 'grdc_no' column to 'station_name' in GeoDataFrame
geojson_gdf = geojson_gdf.rename(columns={'grdc_no': 'station_name'})

# List of common columns to join on
common_columns = ['station_name', 'wmo_reg', 'sub_reg', 'river', 'station', 'country', 'lat', 'long', 'area']

# Merge the DataFrame and GeoDataFrame by the common columns
merged_df = geojson_gdf.merge(csv_df, on=common_columns)

# Output the result to Parquet format
merged_df.to_parquet(output_file)

# End timing the script
end_time = time.time()
elapsed_time = end_time - start_time

print(f'Merged file saved to {output_file}')
print(f'Script executed in {elapsed_time:.2f} seconds')