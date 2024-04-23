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
    filename = get_file_path(fetch_date)
    data_insert = []

    # Get existing IDs from the database
    connection = Connection(db_connection)
    session = connection.get_session()
    existing_ids = {row[0] for row in session.query(Reddit.id).all()}
    session.close()

    with open(filename, encoding='utf-8') as csvf:
        csv_reader = csv.DictReader(csvf)
        for row in csv_reader:
            # Check if ID already exists in the database
            if row['id'] not in existing_ids:
                reddit_data = Reddit(id=row['id'], title=row['title'], author=row['author'], subreddit=row['subreddit'],
                                     upvote_ratio=row['upvote_ratio'], score=row['score'], url=row['url'],
                                     created_date=row['created_date'])
                data_insert.append(reddit_data)
    connection = Connection(db_connection)
    session = connection.get_session()
    session.bulk_save_objects(data_insert)
    session.commit()
    session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=str)
    parser.add_argument("--connection", required=True, type=str)
    args = parser.parse_args()
    main(args.date, args.connection)
