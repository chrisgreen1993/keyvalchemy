"""
test.testdb
"""
from sqlalchemy import create_engine, MetaData, Table, Column, String, PickleType

class TestDB:
    """
    Databse object for testing, creates db, loads it with data
    """

    def __init__(self, db_url, data):
        """
        Connects to db_url and loads with test_data

        Args:
            db_url: database url to connect to
            data: test data as list of dictionaries
        """
        self.engine = create_engine(db_url)
        self.metadata = MetaData()
        self.store = Table('store', self.metadata,
            Column('key', String(255), primary_key=True),
            Column('value', PickleType(protocol=3))
        )
        self.metadata.create_all(self.engine)
        self.conn = self.engine.connect()
        self.conn.execute(self.store.insert(), data)
