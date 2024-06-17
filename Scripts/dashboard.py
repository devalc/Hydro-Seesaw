import pandas as pd
import plotly.express as px
import dash
from dash import dcc, html
from dash.dependencies import Input, Output

# Load the CSV file
file_path = '/Users/sidchaudhary/Documents/GitHub/Hydro-Seesaw/Results/clean_lat_lon_flag_counts_early_mid_end.csv'
data = pd.read_csv(file_path)

# Categorize the flags into 5 classes based on specified ranges
def categorize_flags(flag):
    if flag < 75:
        return '<75'
    elif 75 <= flag < 150:
        return '75-150'
    elif 150 <= flag < 225:
        return '150-225'
    elif 225 <= flag < 300:
        return '225-300'
    else:
        return '>300'

# Initialize the Dash app
app = dash.Dash(__name__)

# Layout of the app
app.layout = html.Div([
    html.H1("FlowView: Visualizing Stream Conditions"),
    
    html.Label("Select Scenario:"),
    dcc.Dropdown(
        id='scenario-dropdown',
        options=[{'label': scenario, 'value': scenario} for scenario in data['scenario'].unique()],
        value='rcp4p5'
    ),
    
    html.Label("Select Time Period:"),
    dcc.Dropdown(
        id='time-period-dropdown',
        options=[{'label': period, 'value': period} for period in data['time_period'].unique()],
        value='mid'
    ),
    
    html.Label("Select Percentile Event:"),
    dcc.Dropdown(
        id='percentile-dropdown',
        options=[
            {'label': 'p1_flag', 'value': 'p1_flag'},
            {'label': 'p5_flag', 'value': 'p5_flag'},
            {'label': 'p10_flag', 'value': 'p10_flag'},
            {'label': 'p90_flag', 'value': 'p90_flag'},
            {'label': 'p95_flag', 'value': 'p95_flag'},
            {'label': 'p99_flag', 'value': 'p99_flag'}
        ],
        value='p1_flag'
    ),
    
    dcc.Graph(id='station-map')
])

# Callback to update the map based on dropdown selections
@app.callback(
    Output('station-map', 'figure'),
    [Input('scenario-dropdown', 'value'),
     Input('time-period-dropdown', 'value'),
     Input('percentile-dropdown', 'value')]
)
def update_map(selected_scenario, selected_time_period, selected_percentile):
    filtered_data = data[
        (data['scenario'] == selected_scenario) & 
        (data['time_period'] == selected_time_period)
    ]
    
    filtered_data['class'] = filtered_data[selected_percentile].apply(categorize_flags)
    
    fig = px.scatter_mapbox(
        filtered_data,
        lat='lat',
        lon='long',
        #hover_name='station_name',
        hover_data={'class': True, 'lat': False, 'long': False},
        color='class',
        category_orders={'class': ['<75', '75-150', '150-225', '225-300', '>300']},
        color_discrete_map={
            '<75': 'blue',
            '75-150': 'green',
            '150-225': 'yellow',
            '225-300': 'orange',
            '>300': 'red'
        },
        zoom=1,  # Set initial zoom level to show the whole world
        center={"lat": 0, "lon": 0},  # Center the map at (0, 0)
        height=600
    )
    
    fig.update_layout(mapbox_style="open-street-map")
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    
    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
