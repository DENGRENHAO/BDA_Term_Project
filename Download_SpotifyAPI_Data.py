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

df = pd.read_csv('./dataset/unique_min_rank.csv', header=0, encoding = "ISO-8859-1")
df = df.drop(['Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15'], axis=1)

df = df.dropna()


tqdm.pandas()
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

def get_all_chunk_features(df_rows):
    global row_cnt, failed_cnt
    row_cnt += 1
    if row_cnt % 1000 == 0:
        print(f"Row Count: {row_cnt}")
    try:
        # if row_cnt == 1:
        #     print(df_rows.head(10))
        chunk_features = sp.audio_features(list(df_rows['url']))
        for i in range(len(chunk_features)):
            df_rows.iloc[i]['danceability'] = chunk_features[i]['danceability']
            df_rows.iloc[i]['energy'] = chunk_features[i]['energy']
            df_rows.iloc[i]['key'] = chunk_features[i]['key']
            df_rows.iloc[i]['loudness'] = chunk_features[i]['loudness']
            df_rows.iloc[i]['mode'] = chunk_features[i]['mode']
            df_rows.iloc[i]['speechiness'] = chunk_features[i]['speechiness']
            df_rows.iloc[i]['acousticness'] = chunk_features[i]['acousticness']
            df_rows.iloc[i]['instrumentalness'] = chunk_features[i]['instrumentalness']
            df_rows.iloc[i]['liveness'] = chunk_features[i]['liveness']
            df_rows.iloc[i]['valence'] = chunk_features[i]['valence']
            df_rows.iloc[i]['tempo'] = chunk_features[i]['tempo']
            df_rows.iloc[i]['duration_ms'] = chunk_features[i]['duration_ms']

        try:
            chunk_track_info = sp.tracks(list(df_rows['url']))
            for i in range(len(chunk_track_info)):
                df_rows.iloc[i]['artist_url'] = chunk_track_info[i]['artists'][0]['external_urls']['spotify']
                df_rows.iloc[i]['track_popularity'] = chunk_track_info[i]['popularity']

            try:
                chunk_artist_info = sp.artist(list(df_rows['artist_url']))
                for i in range(len(chunk_track_info)):
                    df_rows.iloc[i]['artist_genres'] = chunk_artist_info[i]['genres']
                    df_rows.iloc[i]['artist_popularity'] = chunk_artist_info[i]['popularity']

            except Exception as e:
                print(e)
                failed_cnt += 1
        except Exception as e:
            print(e)
            failed_cnt += 1
    except Exception as e:
        print(e)
        failed_cnt += 1

    if failed_cnt > 0 and failed_cnt % 1000 == 0:
        print(f"Failed Count: {failed_cnt}")

    return df_rows


def handle_timeout(signum, frame):
    # This function will be called when the timeout is reached.
    raise Exception("Timeout reached")

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
    
splitted_df = np.array_split(df, 1000)
print(len(splitted_df))
print(splitted_df[0].shape)

split_idx = [i for i in range(1000)]
for i in split_idx:
    row_cnt = 0
    failed_cnt = 0
    chunks = np.array_split(splitted_df[i], 6)
    for j in range(6):
        chunks[j] = get_all_chunk_features_with_timeout(chunks[j])
    # splitted_df[i] = splitted_df[i].progress_apply(get_all_features_with_timeout, axis=1)
    result_df = pd.concat(chunks, axis=0)
    print(f"Row Count: {row_cnt}, Failed Count: {failed_cnt}")
    result_df.to_csv(f"./outputs/final_outputs_{i}.csv", encoding='utf-8', index=False)