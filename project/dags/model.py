"""Set up a SQLAlchemy model representing a table named reddit in the database,
along with necessary functions and classes to manage database connections and initialization."""

from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, String, Integer, DateTime, Float


class Connection(object):

    def __init__(self, db_connection):
        engine = create_engine(db_connection)
        self.engine = engine

    def get_session(self):
        Session = sessionmaker(bind=self.engine)

        return Session()

    def get_engine(self):
        return self.engine


Base = declarative_base()


def init_db(db_connection):
    engine = create_engine(db_connection)
    Base.metadata.create_all(bind=engine)


class Reddit(Base):
    __tablename__ = 'reddit'

    id = Column(String, primary_key=True)
    title = Column(String)
    author = Column(String)
    subreddit = Column(String)
    upvote_ratio = Column(Float)
    score = Column(Integer)
    url = Column(String)
    created_date = Column(DateTime)

    def __init__(self, id, title, author, subreddit, upvote_ratio, score, url, created_date):
        self.id = id
        self.title = title
        self.author = author
        self.subreddit = subreddit
        self.upvote_ratio = upvote_ratio
        self.score = score
        self.url = url
        self.created_date = created_date
