import numpy as np
import spotipy
import spotipy as spotipy
from tqdm import tqdm
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import numpy as np
import signal
import pprint
pp = pprint.PrettyPrinter(indent=4)


client_id = '12e4e34e23e841b3aa386f3e1e9afa74'
client_secret = '1e5335d90d44414495573690e004d3ae'
# client_id = 'dab809ffa57e4712981d068a2dbcc8f6'
# client_secret = 'd0e73dc83d4d45e7aaea970f95de6c23'

client_credentials_manager = SpotifyClientCredentials(client_id = client_id, client_secret = client_secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

tqdm.pandas()
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def get_all_features(df_row):
    global row_cnt, failed_cnt
    row_cnt += 1
    if row_cnt % 1000 == 0:
        print(f"Row Count: {row_cnt}")
    try:
        features = sp.audio_features(df_row['url'])[0]
        df_row['danceability'] = features['danceability']
        df_row['energy'] = features['energy']
        df_row['key'] = features['key']
        df_row['loudness'] = features['loudness']
        df_row['mode'] = features['mode']
        df_row['speechiness'] = features['speechiness']
        df_row['acousticness'] = features['acousticness']
        df_row['instrumentalness'] = features['instrumentalness']
        df_row['liveness'] = features['liveness']
        df_row['valence'] = features['valence']
        df_row['tempo'] = features['tempo']
        df_row['duration_ms'] = features['duration_ms']

        try:
            track_info = sp.track(df_row['url'])
            df_row['artist_url'] = track_info['artists'][0]['external_urls']['spotify']
            df_row['track_popularity'] = track_info['popularity']

            try:
                artist_info = sp.artist(df_row['artist_url'])
                df_row['artist_genres'] = artist_info['genres']
                df_row['artist_popularity'] = artist_info['popularity']

            except Exception as e:
                print(e)
                print("artist failed")
                failed_cnt += 1
        except Exception as e:
            print(e)
            print("track failed")
            failed_cnt += 1
    except Exception as e:
        print(e)
        print("audio_feature failed")
        failed_cnt += 1

    if failed_cnt > 0 and failed_cnt % 1000 == 0:
        print(f"Failed Count: {failed_cnt}")

    return df_row

def get_all_chunk_features(df_rows):
    global chunk_cnt, failed_chunk_cnt
    chunk_cnt += 1
    try:
        features_name = ['danceability', 'energy', 'key', 'loudness', 'mode', 'speechiness', 'acousticness', 'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms', 'artist_url', 'track_popularity', 'artist_genres', 'artist_popularity']
        for feature in features_name:
            df_rows[feature] = np.nan

        chunk_features = sp.audio_features(list(df_rows['url']))
        for i in range(len(chunk_features)):
            df_rows.iloc[i, 10] = chunk_features[i]['danceability']
            df_rows.iloc[i, 11] = chunk_features[i]['energy']
            df_rows.iloc[i, 12] = chunk_features[i]['key']
            df_rows.iloc[i, 13] = chunk_features[i]['loudness']
            df_rows.iloc[i, 14] = chunk_features[i]['mode']
            df_rows.iloc[i, 15] = chunk_features[i]['speechiness']
            df_rows.iloc[i, 16] = chunk_features[i]['acousticness']
            df_rows.iloc[i, 17] = chunk_features[i]['instrumentalness']
            df_rows.iloc[i, 18] = chunk_features[i]['liveness']
            df_rows.iloc[i, 19] = chunk_features[i]['valence']
            df_rows.iloc[i, 20] = chunk_features[i]['tempo']
            df_rows.iloc[i, 21] = chunk_features[i]['duration_ms']

        try:
            chunk_track_info = sp.tracks(list(df_rows['url']))
            for i in range(len(chunk_track_info['tracks'])):
                df_rows.iloc[i, 22] = chunk_track_info['tracks'][i]['artists'][0]['external_urls']['spotify']
                df_rows.iloc[i, 23] = chunk_track_info['tracks'][i]['popularity']

            try:
                chunk_artist_info = sp.artists(list(df_rows['artist_url']))
                df_rows['artist_genres'] = [[] for _ in range(len(df_rows.index))]
                for i in range(len(chunk_artist_info['artists'])):
                    df_rows.at[i, 'artist_genres'] = chunk_artist_info['artists'][i]['genres']
                    df_rows.at[i, 'artist_popularity'] = chunk_artist_info['artists'][i]['popularity']

            except Exception as e:
                print(e)
                print("artists failed")
                failed_chunk_cnt += 1
        except Exception as e:
            print(e)
            print("tracks failed")
            failed_chunk_cnt += 1
    except Exception as e:
        print(e)
        print("audio_features failed")
        failed_chunk_cnt += 1

    return df_rows


def handle_timeout(signum, frame):
    # This function will be called when the timeout is reached.
    raise Exception("Timeout reached")

def get_all_features_with_timeout(row):
    # Set a timeout of 10 seconds.
    signal.signal(signal.SIGALRM, handle_timeout)
    signal.alarm(10)

    try:
        result = get_all_features(row)
        signal.alarm(0)  # Cancel the timeout.
        return result
    except Exception as e:
        print(e)
        signal.alarm(0)  # Cancel the timeout.
        return row

def get_all_chunk_features_with_timeout(rows):
    # Set a timeout of 10 seconds.
    signal.signal(signal.SIGALRM, handle_timeout)
    signal.alarm(120)

    try:
        result = get_all_chunk_features(rows)
        signal.alarm(0)  # Cancel the timeout.
        return result
    except Exception as e:
        print(e)
        signal.alarm(0)  # Cancel the timeout.
        return rows

import os

# Set directory path to the folder containing CSV files
directory_path = "./outputs"

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
print(all_dfs.shape)

# split dataframe of size 296597 to 10 splits
splitted_all_df = np.array_split(all_dfs, 10)

if not os.path.exists('./merged_outputs'):
    os.mkdir('./merged_outputs')
    
last_split_idx = 0

for i in range(last_split_idx, 10):
    df = splitted_all_df[i].copy()
    print(f"original all dataset shape: {df.shape}")
    print(f"row count with nan: {(df.isna().any(axis=1)).sum()}")
    row_cnt, failed_cnt = 0, 0
    df[df.isna().any(axis=1)] = df[df.isna().any(axis=1)].progress_apply(get_all_features_with_timeout, axis=1)
    print(f"Row Count: {row_cnt}, Failed Count: {failed_cnt}")
    print(f"row count with nan after processed: {(df.isna().any(axis=1)).sum()}")
    df = df.dropna()
    print(f"all dataset shape after processed: {df.shape}")
    df.to_csv(f"./outputs/all_final_outputs_{i}.csv", encoding='utf-8', index=False)