#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec  2 13:29:15 2024

@author: cdeval
"""

import pandas as pd
import plotly.express as px
import streamlit as st
import geopandas as gpd

# app page configuration
st.set_page_config(
    page_title="FlowView: Visualizing Stream Conditions",
    layout="wide",  
    initial_sidebar_state="expanded"  
)

# Load the CSV file
file_path = 'app/data/lat_lon_flag_counts_early_mid_end_season.parquet'
data = pd.read_parquet(file_path)

# Load the Parquet 
parquet_file_path = 'app/data/IPCC-WGI-reference-regions-v4.parquet'
parquet_data = gpd.read_parquet(parquet_file_path)

parquet_data = parquet_data.set_crs('EPSG:4326', allow_override=True)

# Categorization for historical data
def categorize_flags_hist(flag):
    if flag < 5:
        return "<5"
    elif 5 <= flag < 10:
        return "5-10"
    elif 10 <= flag < 20:
        return "10-20"
    elif 20 <= flag < 50:
        return "20-50"
    elif 50 <= flag < 100:
        return "50-100"
    else:
        return ">100"

# Categorization for future scenarios data 
def categorize_flags_non_hist(flag):
    if flag < 75:
        return "<75"
    elif 75 <= flag < 150:
        return "75-150"
    elif 150 <= flag < 225:
        return "150-225"
    elif 225 <= flag < 300:
        return "225-300"
    elif 300 <= flag < 375:
        return "300-375"
    else:
        return ">375"

# App title
st.title("FlowView: Visualizing Stream Conditions")

# Sidebar for input controls
st.sidebar.header("Filter Options")

# Dropdown: Select Scenario
scenario_options = data['scenario'].unique()
selected_scenario = st.sidebar.selectbox("Select Scenario:", scenario_options)

# Dropdown: Select Time Period 
if selected_scenario == 'hist':
    time_period_options = ['historical']
else:
    time_period_options = [period for period in data['time_period'].unique() if period != 'historical']
selected_time_period = st.sidebar.selectbox("Select Time Period:", time_period_options)

# Dropdown: Select Percentile Event
percentile_options = ['p1_flag', 'p5_flag', 'p10_flag', 'p90_flag', 'p95_flag', 'p99_flag']
selected_percentile = st.sidebar.selectbox("Select Percentile Event:", percentile_options)

# Dropdown: Select Season/Annual

# Recode the seasons column
season_mapping = {
    'Annual': 'Annual',
    'Season 1': 'Season 1 (DJF)',
    'Season 2': 'Season 2 (MAM)',
    'Season 3': 'Season 3 (JJA)',
    'Season 4': 'Season 4 (SON)'
}

# Apply the mapping to the 'season' column
data['season'] = data['season'].replace(season_mapping)
season_options = data['season'].unique()

selected_season = st.sidebar.selectbox("Select Season/Annual:", season_options)

# Filter the data based on selections
filtered_data = data[
    (data['scenario'] == selected_scenario) &
    (data['time_period'] == selected_time_period) &
    (data['season'] == selected_season)
]

# Apply appropriate categorization
if selected_scenario == 'hist':
    filtered_data['class'] = filtered_data[selected_percentile].apply(categorize_flags_hist)
else:
    filtered_data['class'] = filtered_data[selected_percentile].apply(categorize_flags_non_hist)

# Define fixed color mappings for categories
if selected_scenario == 'hist':
    color_map = {
    "<5": "blue",
    "5-10": "green",
    "10-20": "yellow",
    "20-50": "orange",
    "50-100": "purple",
    ">100": "red"
}
    category_order = ["<5", "5-10", "10-20", "20-50", "50-100", ">100"]
else:
    color_map = {
        "<75": "blue",
        "75-150": "green",
        "150-225": "yellow",
        "225-300": "orange",
        "300-375": "purple",
        ">375": "red"
    }
    category_order = ["<75", "75-150", "150-225", "225-300", "300-375", ">375"]

# Create scatter mapbox figure for the main data (CSV)
fig = px.scatter_mapbox(
    filtered_data,
    lat='lat',
    lon='long',
    hover_data={
        'class': True,             
        selected_percentile: True, 
        'river': True,             
        'station': True,          
        'country': True,           
        'station_name': True,      
        'lat': False,              
        'long': False              
    },
    color='class',                
    color_discrete_map=color_map,  #
    category_orders={'class': category_order},  
    zoom=2,                        
    center={"lat": 0, "lon": 0},  
    height=900                    
)

# Add Parquet data layer as polygons
# Iterate over the rows in the Parquet data and add polygons to the map
for _, row in parquet_data.iterrows():
    geometry = row['geometry']
    
    if geometry.geom_type == 'Polygon':
        # For Polygon geometry, extract the exterior coordinates
        coords = geometry.exterior.coords
        lon, lat = zip(*coords)
        fig.add_scattermapbox(
            fill="none",  # Fill the polygon
            fillcolor="rgba(0, 0, 255, 0.1)",  # Blue with transparency
            line=dict(color="grey", width=0.1),
            lon=lon,
            lat=lat,
            hovertext=row['Name'],
            showlegend=False, 
            #name=row['Name'],  # Name the layer for legend
        )
    elif geometry.geom_type == 'MultiPolygon':
        # For MultiPolygon geometry, iterate over each Polygon in the MultiPolygon
        for poly in geometry.geoms:  # .geoms returns a list of individual Polygons
            coords = poly.exterior.coords
            lon, lat = zip(*coords)
            fig.add_scattermapbox(
                fill="none",  # Fill the polygon
                fillcolor="rgba(0, 0, 255, 0.1)",  # Blue with transparency
                line=dict(color="grey", width=0.1),  # Border color and width
                lon=lon,
                lat=lat,
                hovertext=row['Name'],
                showlegend=False, 
                #name=row['Name'],  # Name the layer for legend
            )



# Improve map layout
fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})

# Display the map
st.plotly_chart(fig, use_container_width=True)
