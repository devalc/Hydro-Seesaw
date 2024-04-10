import dask.dataframe as dd
import pandas as pd

# Read Parquet file into a Dask DataFrame
ddf = dd.read_parquet('C://Users//Chinmay//Downloads//grouped_median_discharge.parquet')

# Initialize an empty list to store intermediate results
intermediate_results = []

# Define chunk size (adjust as needed)
chunk_size = 100000  # for example

# Iterate over chunks of the DataFrame
for chunk in ddf.to_delayed():
    # Process chunk and compute missing values
    result = chunk.compute().groupby(['station_name', 'scenario']).apply(lambda x: x.isna().sum())
    # Append intermediate result to the listimport dask.dataframe as dd
import pandas as pd

# Read Parquet file into a Dask DataFrame
ddf = dd.read_parquet('C://Users//Chinmay//Downloads//grouped_median_discharge.parquet')

# Initialize an empty list to store intermediate results
intermediate_results = []

# Define chunk size (adjust as needed)
chunk_size = 100000  # for example

# Iterate over chunks of the DataFrame
for chunk in ddf.to_delayed():
    # Process chunk and compute missing values
    result = chunk.compute().groupby(['station_name', 'scenario']).apply(lambda x: x.isna().sum(), include_groups=False)
    # Append intermediate result to the list
    intermediate_results.append(result)

# Concatenate intermediate results into a single DataFrame
missing_values_grouped = pd.concat(intermediate_results)

# Reset index to ensure 'station_name' is not set as an index
missing_values_grouped.reset_index(inplace=True)

# Aggregate results
missing_values_grouped = missing_values_grouped.groupby(['station_name', 'scenario']).sum()

# Write the DataFrame to an Excel file
output_excel_file = 'missing_values_analysis.xlsx'
missing_values_grouped.to_excel(output_excel_file)

print(f"Output Excel file '{output_excel_file}' has been created.")
