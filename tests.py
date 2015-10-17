import unittest
from sqlalchemy.sql import select, func
from keyvalchemy import KeyValchemy

class KeyValchemyTestCase(unittest.TestCase):

    def setUp(self):
        self.db = KeyValchemy('sqlite:///:memory:')
        self.db.conn.execute(self.db.store.insert(), [
            {'key': 'key1', 'value': 'string'},
            {'key': 'key2', 'value': [1,2,3,4,5]},
            {'key': 'key3', 'value': {'k1': 'v1', 'k2': [('stuff',1),(30,20)]}},
        ])

    def tearDown(self):
        self.db.metadata.drop_all(self.db.engine)
        self.db.close()

    def test___init__(self):
        self.assertEqual(1, len(self.db.metadata.tables))
        self.assertIn('store', self.db.metadata.tables)
        self.assertEqual(2, len(self.db.store.columns))

    def test___getitem__(self):
        # Doesnt exist
        self.assertRaises(KeyError, self.db.__getitem__, 'key')
        # Exists
        self.assertEqual(self.db['key1'], 'string')
        self.assertEqual(self.db['key2'], [1,2,3,4,5])

    def test___setitem__(self):
        # Exists
        result = self.db.conn.execute(select([self.db.store.c.value]).where(self.db.store.c.key == 'key1')).fetchone()['value']
        self.assertEqual('string', result)
        value = ['list', 'of', 'strings']
        self.db['key1'] = value
        result = self.db.conn.execute(select([self.db.store.c.value]).where(self.db.store.c.key == 'key1')).fetchone()['value']
        self.assertEqual(value, result)
        # Doesnt exist
        result = self.db.conn.execute(select([self.db.store.c.value]).where(self.db.store.c.key == 'nope')).fetchone()
        self.assertIsNone(result)
        value = (1, 'stuff', ['test', 'ing'])
        self.db['nope'] = value
        result = self.db.conn.execute(select([self.db.store.c.value]).where(self.db.store.c.key == 'nope')).fetchone()['value']
        self.assertEqual(value, result)

    def test___delitem__(self):
        # Doesnt exist
        self.assertRaises(KeyError, self.db.__delitem__, 'key')
        # Exists
        result = self.db.conn.execute(select([self.db.store.c.value]).where(self.db.store.c.key == 'key2')).fetchone()['value']
        self.assertEqual([1,2,3,4,5], result)
        del self.db['key2']
        result = self.db.conn.execute(select([self.db.store.c.value]).where(self.db.store.c.key == 'key2')).fetchone()
        self.assertIsNone(result)

    def test___len__(self):
        self.assertEqual(3, len(self.db))
        self.db['another'] = 'testing'
        self.assertEqual(4, len(self.db))

    def test___contains__(self):
        # exists
        self.assertTrue('key1' in self.db)
        # doesnt exist
        self.assertFalse('nope' in self.db)

    def test_keys(self):
        self.assertEqual(['key1', 'key2', 'key3'], list(self.db.keys()))
        # empty
        self.db.conn.execute(self.db.store.delete())
        self.assertEqual([], list(self.db.keys()))

    def test_values(self):
        self.assertEqual(['string', [1,2,3,4,5], {'k1': 'v1', 'k2': [('stuff',1),(30,20)]}], list(self.db.values()))
        # empty
        self.db.conn.execute(self.db.store.delete())
        self.assertEqual([], list(self.db.values()))

    def test_items(self):
        self.assertEqual([('key1', 'string'), ('key2', [1,2,3,4,5]), ('key3', {'k1': 'v1', 'k2': [('stuff',1),(30,20)]})], list(self.db.items()))
        # empty
        self.db.conn.execute(self.db.store.delete())
        self.assertEqual([], list(self.db.items()))

    def test_clear(self):
        count = self.db.conn.execute(select([func.count()]).select_from(self.db.store)).fetchone()[0]
        self.assertEqual(3, count)
        self.db.clear()
        count = self.db.conn.execute(select([func.count()]).select_from(self.db.store)).fetchone()[0]
        self.assertEqual(0, count)


if __name__ == '__main__':
    unittest.main()
