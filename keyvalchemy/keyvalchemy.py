"""
keyvalchemy.keyvalchemy
"""
import collections
from sqlalchemy import create_engine, MetaData, Table, Column, String, PickleType
from sqlalchemy.sql import select, func, exists


class ConnectionClosedError(ValueError):
    """
    More specific exception if connecion closed
    """
    pass

class KeyValchemy(collections.MutableMapping):
    """
    Object that allows database to be accessed like a dictionary, works the same
    as using a normal python dictionary
    """

    def __init__(self, db_url, protocol=3, engine_kwargs=None):
        """
        Constructor - Connects to db_url and creates table if it doesn't exist

        Args:
            db_url: database url to use e.g sqlite:///test.db
            protocol: pickle protocol to use (defaults to 3)
            engine_kwargs: dict of keyword args to be passed into sqlalchemy.create_engine()
        """
        if engine_kwargs is None:
            engine_kwargs = {}
        self.engine = create_engine(db_url, **engine_kwargs)
        self.metadata = MetaData()
        self.store = Table('store', self.metadata,
            Column('key', String(255), primary_key=True),
            Column('value', PickleType(protocol=protocol))
        )
        self.metadata.create_all(self.engine)
        self.conn = self.engine.connect()

    def __getitem__(self, key):
        """
        Implements dictionary get (db['key'])
        Fetches value from database if it exists else raises KeyError

        Args:
            key: key to get value from
        Returns:
            Unpickled value that was at key
        Raises:
            KeyError: If key doesn't exist
        """
        query = select([self.store.c.value]).where(self.store.c.key == key)
        result = self.conn.execute(query).fetchone()
        if result:
            return result['value']
        raise KeyError

    def __setitem__(self, key, value):
        """
        Implements dictionary set (db['key'] = obj)
        Stores value at key in the database, if key already exists it is overwritten

        Args:
            key: key to store value at
            value: python object to store
        """
        query = self.store.update().where(self.store.c.key == key).values(value=value)
        result = self.conn.execute(query)
        if result.rowcount == 0:
            query = self.store.insert().values(key=key, value=value)
            result = self.conn.execute(query)

    def __delitem__(self, key):
        """
        Implements dictionary item delete (del db['key'])
        Stores value at key in the database, if key already exists it is overwritten

        Args:
            key: key to delete
        Raises:
            KeyError: If key doesn't exist in database
        """
        query = self.store.delete().where(self.store.c.key == key)
        result = self.conn.execute(query)
        if result.rowcount == 0:
            raise KeyError

    def __contains__(self, key):
        """
        Implements contains semantics ('key' in db)
        Checks whether key exists in database

        Args:
            key: key to check for existence
        Returns:
            True if it exists else False
        """
        query = select([exists().where(self.store.c.key == key)])
        result = self.conn.execute(query)
        return result.fetchone()[0]

    def keys(self):
        """
        Creates generator containing keys in the database

        Returns:
            generator that iterates through keys in database
        """
        query = select([self.store.c.key])
        result = self.conn.execute(query)
        for row in result:
            yield row['key']

    def values(self):
        """
        Creates generator containing values in the database

        Returns:
            generator that iterates through values in database
        """
        query = select([self.store.c.value])
        result = self.conn.execute(query)
        for row in result:
            yield row['value']

    def items(self):
        """
        Creates generator containing items in the database as (key, value) tuples

        Returns:
            generator that iterates through items in database
        """
        query = select([self.store])
        result = self.conn.execute(query)
        for row in result:
            yield row

    def clear(self):
        """
        Deletes all values in database
        """
        query = self.store.delete()
        self.conn.execute(query)

    def __iter__(self):
        """
        Implements iterator semantics (for i in db)

        Returns:
            generator from keys() method
        """
        return self.keys()

    def __len__(self):
        """
        Implements len(db) semantics
        Counts no of values in database

        Returns:
            No of values in database
        """
        query = select([func.count()]).select_from(self.store)
        result = self.conn.execute(query)
        return result.fetchone()[0]

    def __enter__(self):
        """
        Implements with statement (with keyvalchemy.open(db_url) as db)
        """
        return self

    def __exit__(self, type, value, traceback):
        """
        Close connection on exiting with statement
        """
        self.close()

    def __del__(self):
        """
        Destructor - closes connection
        """
        self.close()

    def close(self):
        """
        Closes connection to database by calling SQLAlchemy Connection.close() method
        Replaces object with ClosedKeyValchemy object which raises exceptions
        if user tries to do anything with the db once it is closed
        """
        if isinstance(self, ClosedKeyValchemy):
            return
        self.conn.close()
        self.__class__ = ClosedKeyValchemy

class ClosedKeyValchemy(KeyValchemy):
    """
    Class to replace KeyValchemy when connection to db is closed
    Raises ConnectionClosedError if user tries to call methods
    """
    def closed(*args, **kwargs):
        raise ConnectionClosedError('Database connection is closed')

    # Methods to raise ConnectionClosedError on
    __getitem__ = __setitem__ = __delitem__ = __contains__ = keys = values = items = __len__ = closed
