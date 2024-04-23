import requests
import argparse
import os
from datetime import timedelta, datetime
import pandas as pd
import config
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)


def get_yesterday_date(fetch_date):
    return datetime.strptime(fetch_date, '%Y-%m-%d').date() - timedelta(1)


def get_file_path(fetch_date):
    yesterday = get_yesterday_date(fetch_date)
    filename = f"reddit_{yesterday}.csv"
    return os.path.join(config.CSV_FILE_DIR, filename)


def import_data():
    url = config.REDDIT_API
    headers = config.HEADERS
    response = requests.get(url, headers=headers)
    return response.json()


def transform_data(data_json):
    posts = [post['data'] for post in data_json['data']['children']]
    df = pd.DataFrame(posts)
    df['created_date'] = pd.to_datetime(df['created_utc'], unit='s')
    return df


def save_new_data_to_csv(df, fetch_date):
    filename = get_file_path(fetch_date)
    if not df.empty:
        df.to_csv(filename, encoding='utf-8', index=False)


def main(fetch_date):
    data_json = import_data()
    df = transform_data(data_json)
    save_new_data_to_csv(df, fetch_date)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str)
    args = parser.parse_args()
    main(args.date)
