import os
import pandas as pd

# Set directory path to the folder containing CSV files
directory_path = "./merged_outputs"

# Initialize an empty list to store dataframes
dfs = []

# Loop over each file in the directory
for filename in os.listdir(directory_path):
    if filename.endswith(".csv"):
        # Read the CSV file into a dataframe
        filepath = os.path.join(directory_path, filename)
        df = pd.read_csv(filepath, header=0, encoding = "utf-8")

        # Add the dataframe to the list
        dfs.append(df)

# Concatenate all dataframes into a single dataframe
all_dfs = pd.concat(dfs, ignore_index=True)
print(f"all dataset shape before dropping nan processed: {all_dfs.shape}")

all_dfs = all_dfs.dropna()
print(f"all dataset shape after dropping nan: {all_dfs.shape}")
all_dfs.to_csv(f"./dataset/spotify_chart_and_api_dataset.csv", encoding='utf-8', index=False)