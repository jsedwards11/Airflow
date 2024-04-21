"""Retrieve data from a CSV file, transform it into instances of the Reddit class, perform data manipulation and
cleaning, delete existing data for yesterday's date from the database, and insert new data into the reddit table.
Designed to be run from the command line with the --date and --connection arguments specifying
the fetch date and database connection string, respectively."""

import argparse
import os
import csv
from datetime import timedelta, datetime

from model import Connection, Reddit
import config


def get_yesterday_date(fetch_date):
    return datetime.strptime(fetch_date, '%Y-%m-%d').date() - timedelta(1)


def get_file_path(fetch_date):
    yesterday = get_yesterday_date(fetch_date)
    filename = "reddit_{}.csv".format(yesterday)
    return os.path.join(config.CSV_FILE_DIR, filename)


def main(fetch_date, db_connection):
    yesterday = get_yesterday_date(fetch_date)
    filename = get_file_path(fetch_date)
    data_insert = []

    with open(filename, encoding='utf-8') as csvf:
        csv_reader = csv.DictReader(csvf)
        for row in csv_reader:
            reddit_data = Reddit(id=row['id'], title=row['title'], author=row['author'], subreddit=row['subreddit'],
                                 upvote_ratio=row['upvote_ratio'], score=row['score'], url=row['url'],
                                 created_date=row['created_date'])
            data_insert.append(reddit_data)

    connection = Connection(db_connection)
    session = connection.get_session()
    session.execute("DELETE FROM reddit where created_date >= timestamp '{} 00:00:00'".format(yesterday, fetch_date))
    session.bulk_save_objects(data_insert)
    session.commit()
    session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str)
    parser.add_argument("--connection", required=True, type=str)
    args = parser.parse_args()
    main(args.date, args.connection)
