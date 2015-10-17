import collections
from sqlalchemy import create_engine, MetaData, Table, Column, String, PickleType
from sqlalchemy.sql import select, func, exists


class KeyValchemy(collections.MutableMapping):

    def __init__(self, db_url, protocol=3, engine_kwargs=None):
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
        query = select([self.store.c.value]).where(self.store.c.key == key)
        result = self.conn.execute(query).fetchone()
        if result:
            return result['value']
        raise KeyError

    def __setitem__(self, key, value):
        query = self.store.update().where(self.store.c.key == key).values(value=value)
        result = self.conn.execute(query)
        if result.rowcount == 0:
            query = self.store.insert().values(key=key, value=value)
            result = self.conn.execute(query)

    def __delitem__(self, key):
        query = self.store.delete().where(self.store.c.key == key)
        result = self.conn.execute(query)
        if result.rowcount == 0:
            raise KeyError
    """
    def __contains__(self, key):
        query = select([exists().where(self.store.c.key == key)])
        result = self.conn.execute(query)
        return result.fetchone()[0]
    """

    def keys(self):
        query = select([self.store.c.key])
        result = self.conn.execute(query)
        for row in result:
            yield row['key']

    def values(self):
        query = select([self.store.c.value])
        result = self.conn.execute(query)
        for row in result:
            yield row['value']

    def items(self):
        query = select([self.store])
        result = self.conn.execute(query)
        for row in result:
            yield row

    def clear(self):
        query = self.store.delete()
        self.conn.execute(query)

    def __iter__(self):
        return self.keys()

    def __len__(self):
        query = select([func.count()]).select_from(self.store)
        result = self.conn.execute(query)
        return result.fetchone()[0]

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        self.conn.close()
