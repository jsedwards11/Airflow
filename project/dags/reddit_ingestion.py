"""Retrieve data from Reddit API, process it, and filter it to include only data from yesterday, then save it to a CSV
file. Designed to be run from the command line with the --date argument specifying the fetch date."""

import requests
import argparse
import os

from datetime import timedelta, datetime
import pandas as pd

import config

# rename_header = {'ID': 'id',
#                  'Title': 'title',
#                  'Author': 'author',
#                  'Subreddit': 'subreddit',
#                  'Upvotes': 'upvotes',
#                  'Score': 'score',
#                  'UR:': 'url',
#                  'Created Date': 'created_date'}
# reposition_header = ['timestamp', 'date_time', 'traffic_index', 'jams_count', 'jams_length', 'jams_delay', 'traffic_index_weekago']


def get_yesterday_date(fetch_date):
    return datetime.strptime(fetch_date, '%Y-%m-%d').date() - timedelta(1)


def get_file_path(fetch_date):
    yesterday = get_yesterday_date(fetch_date)
    filename = "reddit_{}.csv".format(yesterday)
    return os.path.join(config.CSV_FILE_DIR, filename)


def import_data():
    url = config.REDDIT_API
    headers = config.HEADERS
    data_req = requests.get(url, headers=headers)
    data_json = data_req.json()
    return data_json


def transform_data(data_json):
    posts = [post['data'] for post in data_json['data']['children']]
    df = pd.DataFrame(posts)
    df['created_date'] = pd.to_datetime(df['created_utc'], unit='s')
    return df


def get_new_data(df, fetch_date):
    yesterday = get_yesterday_date(fetch_date)
    df = df.sort_values(by=['created_date'], ascending=True)
    data_to_append = df[(df['created_date'].dt.date == yesterday)]
    return data_to_append


def save_new_data_to_csv(data_to_append, fetch_date):
    filename = get_file_path(fetch_date)
    if not data_to_append.empty:
        data_to_append.to_csv(filename, encoding='utf-8', index=False)


def main(fetch_date):
    data_json = import_data()
    df = transform_data(data_json)
    data_to_append = get_new_data(df, fetch_date)
    save_new_data_to_csv(data_to_append, fetch_date)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str)
    args = parser.parse_args()
    main(args.date)
