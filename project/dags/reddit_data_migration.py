"""Initialize database table structure for storing Reddit data. Create necessary table if it doesn't exist and ensure
required directory for CSV files exists. The database connection string is provided as a command-line argument."""

import argparse
from pathlib import Path

from model import Connection
import config


# Initialize Reddit Table
def main(db_connection):
    Path(config.CSV_FILE_DIR).mkdir(parents=True, exist_ok=True)

    connection = Connection(db_connection)
    session = connection.get_session()
    session.execute('''CREATE TABLE IF NOT EXISTS reddit (
    id VARCHAR(100) PRIMARY KEY,
    title VARCHAR(250), 
    author VARCHAR(100), 
    subreddit VARCHAR(50), 
    upvote_ratio DECIMAL, 
    score INT, 
    comments INT, 
    url VARCHAR(250),
    created_date TIMESTAMP)''')
    session.commit()
    session.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--connection", required=True, type=str)
    args = parser.parse_args()
    main(args.connection)
