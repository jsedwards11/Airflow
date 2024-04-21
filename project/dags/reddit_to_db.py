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
    # Delete existing data for yesterday's date where the id already exists
    existing_ids = tuple(reddit.id for reddit in data_insert)  # Convert list to tuple
    placeholders = ', '.join([f"'{id_}'" for id_ in existing_ids])  # Surround each id with quotes
    query = f"DELETE FROM reddit WHERE created_date >= '{yesterday} 00:00:00' AND id IN ({placeholders})"
    session.execute(query)

    # Insert new data into the reddit table
    session.bulk_save_objects(data_insert)
    session.commit()
    session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str)
    parser.add_argument("--connection", required=True, type=str)
    args = parser.parse_args()
    main(args.date, args.connection)
