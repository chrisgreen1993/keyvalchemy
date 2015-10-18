"""
test.test_keyvalchemy
"""
import os
import unittest
from sqlalchemy.sql import select, func
from keyvalchemy import KeyValchemy, ClosedKeyValchemy, ConnectionClosedError
from .testdb import TestDB

TEST_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DB = 'test.db'

class KeyValchemyTestCase(unittest.TestCase):
    """
    Unit tests for KeyValchemy class
    """

    def setUp(self):
        """
        Called before each test, creates test_db database object for updating db
        and initialises KeyValchemy object
        """
        os.environ.setdefault('DB_URL', 'sqlite:///' + os.path.join(TEST_DIR, TEST_DB))
        test_data = [
            {'key': 'key1', 'value': 'string'},
            {'key': 'key2', 'value': [1,2,3,4,5]},
            {'key': 'key3', 'value': {'k1': 'v1', 'k2': [('stuff',1),(30,20)]}},
        ]
        self.test_db = TestDB(os.environ['DB_URL'], test_data)
        self.db = KeyValchemy(os.environ['DB_URL'])

    def tearDown(self):
        """
        Called after each test, drops tables, closes connections and deletes
        db file
        """
        self.test_db.metadata.drop_all(self.test_db.engine)
        self.test_db.conn.close()
        self.db.close()
        if os.environ['DB_URL'][:6] == 'sqlite':
            os.remove(os.path.join(TEST_DIR, TEST_DB))

    def test___init__(self):
        """
        Tests __init__, checks to see if db has correct tables
        """
        self.assertIsInstance(self.db, KeyValchemy)
        self.assertEqual(1, len(self.db.metadata.tables))
        self.assertIn('store', self.db.metadata.tables)
        self.assertEqual(2, len(self.db.store.columns))

    def test___getitem__(self):
        """
        Tests __getitem__ fetches values from db or raises KeyError
        """
        # Doesnt exist
        self.assertRaises(KeyError, self.db.__getitem__, 'key')
        # Exists
        self.assertEqual(self.db['key1'], 'string')
        self.assertEqual(self.db['key2'], [1,2,3,4,5])

    def test___setitem__(self):
        """
        Tests __setitem__ saves to db
        """
        # Exists
        result = self.test_db.conn.execute(select([self.test_db.store.c.value]).where(self.test_db.store.c.key == 'key1')).fetchone()['value']
        self.assertEqual('string', result)
        value = ['list', 'of', 'strings']
        self.db['key1'] = value
        result = self.test_db.conn.execute(select([self.test_db.store.c.value]).where(self.test_db.store.c.key == 'key1')).fetchone()['value']
        self.assertEqual(value, result)
        # Doesnt exist
        result = self.test_db.conn.execute(select([self.test_db.store.c.value]).where(self.test_db.store.c.key == 'nope')).fetchone()
        self.assertIsNone(result)
        value = (1, 'stuff', ['test', 'ing'])
        self.db['nope'] = value
        result = self.test_db.conn.execute(select([self.test_db.store.c.value]).where(self.test_db.store.c.key == 'nope')).fetchone()['value']
        self.assertEqual(value, result)

    def test___delitem__(self):
        """
        Tests __delitem__ deletes from db or raises KeyError
        """
        # Doesnt exist
        self.assertRaises(KeyError, self.db.__delitem__, 'key')
        # Exists
        result = self.test_db.conn.execute(select([self.test_db.store.c.value]).where(self.test_db.store.c.key == 'key2')).fetchone()['value']
        self.assertEqual([1,2,3,4,5], result)
        del self.db['key2']
        result = self.test_db.conn.execute(select([self.test_db.store.c.value]).where(self.test_db.store.c.key == 'key2')).fetchone()
        self.assertIsNone(result)

    def test___len__(self):
        """
        Tests __len__ gets correct no of records
        """
        self.assertEqual(3, len(self.db))
        self.test_db.conn.execute(self.test_db.store.insert().values(key='another', value='testing'))
        self.assertEqual(4, len(self.db))

    def test___contains__(self):
        """
        Tests __contains__ returns whether a key exists
        """
        # exists
        self.assertTrue('key1' in self.db)
        # doesnt exist
        self.assertFalse('nope' in self.db)

    def test_keys(self):
        """
        Tests keys returns correct keys for db
        """
        self.assertEqual(['key1', 'key2', 'key3'], list(self.db.keys()))
        # empty
        self.test_db.conn.execute(self.test_db.store.delete())
        self.assertEqual([], list(self.db.keys()))

    def test_values(self):
        """
        Tests values returns correct values for db
        """
        self.assertEqual(['string', [1,2,3,4,5], {'k1': 'v1', 'k2': [('stuff',1),(30,20)]}], list(self.db.values()))
        # empty
        self.test_db.conn.execute(self.test_db.store.delete())
        self.assertEqual([], list(self.db.values()))

    def test_items(self):
        """
        Tests items returns correct items for db
        """
        self.assertEqual([('key1', 'string'), ('key2', [1,2,3,4,5]), ('key3', {'k1': 'v1', 'k2': [('stuff',1),(30,20)]})], list(self.db.items()))
        # empty
        self.test_db.conn.execute(self.test_db.store.delete())
        self.assertEqual([], list(self.db.items()))

    def test_clear(self):
        """
        Tests clear removes all values from db
        """
        count = self.test_db.conn.execute(select([func.count()]).select_from(self.test_db.store)).fetchone()[0]
        self.assertEqual(3, count)
        self.db.clear()
        count = self.test_db.conn.execute(select([func.count()]).select_from(self.test_db.store)).fetchone()[0]
        self.assertEqual(0, count)

    def test_with_statement(self):
        """
        Tests __enter__ and __exit__ work as intended
        """
        with KeyValchemy(os.environ['DB_URL']) as kv:
            self.assertIsInstance(self.db, KeyValchemy)
            self.assertEqual(1, len(self.db.metadata.tables))
            self.assertIn('store', self.db.metadata.tables)
            self.assertEqual(2, len(self.db.store.columns))
        # db connection should now be closed
        self.assertTrue(kv.conn.closed)
        self.assertIsInstance(kv, ClosedKeyValchemy)

    def test_close(self):
        """
        Tests close() closes connection and replaces KeyValchemy with ClosedKeyValchemy
        """
        self.assertFalse(self.db.conn.closed)
        self.db.close()
        self.assertTrue(self.db.conn.closed)
        self.assertIsInstance(self.db, ClosedKeyValchemy)
        # Check exceptions raises on methods
        self.assertRaises(ConnectionClosedError, self.db.__getitem__, 'key')
        self.assertRaises(ConnectionClosedError, self.db.__setitem__, 'key', 'val')
        self.assertRaises(ConnectionClosedError, self.db.__delitem__, 'key')
        self.assertRaises(ConnectionClosedError, self.db.__contains__, 'key')
        self.assertRaises(ConnectionClosedError, self.db.keys)
        self.assertRaises(ConnectionClosedError, self.db.values)
        self.assertRaises(ConnectionClosedError, self.db.items)
        self.assertRaises(ConnectionClosedError, self.db.__len__)
        # Shouldnt raise exception
        self.db.close()

class ClosedKeyValchemyTestCase(unittest.TestCase):
    """
    Unit tests for ClosedKeyValchemy class
    """

    def test_closed(self):
        """
        Tests whether closed method raises ConnectionClosedError
        """
        self.assertRaises(ConnectionClosedError, ClosedKeyValchemy.closed)

if __name__ == '__main__':
    unittest.main()
