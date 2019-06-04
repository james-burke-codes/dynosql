#!env/bin/python3
import unittest
import sys

import logging

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s:%(name)s:%(lineno)s:%(levelname)s - %(message)s",
                    level="INFO")

from dynosql import dynosql

class FunctionalTestCase(unittest.TestCase):

    def setUp(self):
        self.dyno = dynosql.Dynosql()
        self.tables = {}

    def tearDown(self):
        for k in list(self.tables):
            print('deleting:', k)
            self.tables[k].drop()
        print('show tables:', self.tables)

    def test_create_table(self):
        self.tables['music'] = self.dyno(table_name='music', partition_key=('artist', 'str',), sort_key=('song', 'str',))
        self.assertEqual(self.tables['music'].info['Table']['TableName'], 'music')

    def test_delete_table(self):
        self.tables['music'] = self.dyno(table_name='music', partition_key=('artist', 'str',), sort_key=('song', 'str',))
        del self.tables['music']
        self.assertNotIn('music1', self.dyno.keys())

    def test_insert_record_with_sort_key(self):
        self.tables['music'] = self.dyno(table_name='music', partition_key=('artist', 'str',), sort_key=('song', 'str',))
        self.tables['music']['Prince', 'Purple Rain'] = {'released': 1984, 'album': 'Purple Rain'}
        self.assertEqual(self.tables['music']['Prince', 'Purple Rain']['released'], 1984)

    def test_insert_record_without_sort_key(self):
        self.tables['music'] = self.dyno(table_name='music', partition_key=('song', 'str',))
        self.tables['music']['Prince - Purple Rain'] = {'released': 1984, 'album': 'Purple Rain'}
        self.assertEqual(self.tables['music']['Prince - Purple Rain']['released'], 1984)

    def test_insert_record_with_sort_key_but_retreive_without(self):
        self.tables['music'] = self.dyno(table_name='music', partition_key=('artist', 'str',), sort_key=('song', 'str',))
        self.tables['music']['Prince', 'Purple Rain'] = {'released': 1984, 'album': 'Purple Rain'}
        self.assertRaises(KeyError)

    def test_insert_record_without_sort_key_but_retreive_with(self):
        self.tables['music'] = self.dyno(table_name='music', partition_key=('song', 'str',))
        self.tables['music']['Prince - Purple Rain'] = {'released': 1984, 'album': 'Purple Rain'}
        self.assertRaises(KeyError)

    def test_reference_existing_table(self):
        self.tables['music'] = self.dyno(table_name='music', partition_key=('song', 'str',))
        self.tables['music']['Prince - Purple Rain'] = {'released': 1984, 'album': 'Purple Rain'}

        self.tables['music2'] = self.dyno(table_name='music')
        self.assertEqual(self.tables['music2']['Prince - Purple Rain']['released'], 1984)

    # def test_update_record_argument(self):
    #     self.tables['music'] = self.dyno(table_name='music', partition_key=('artist', 'str',), sort_key=('song', 'str',))
    #     logger.info('create record')
    #     self.tables['music']['Prince', 'Purple Rain'] = {'released': 1983, 'album': 'Purple Rain'}
    #     logger.info('update record')
    #     self.tables['music']['Prince', 'Purple Rain']['released'] = 1984
    #     logger.info('update ended')
    #     logger.info(self.tables['music']['Prince', 'Purple Rain'])
    #     assert False
    #     self.assertEqual(self.tables['music']['Prince', 'Purple Rain']['released'], 1984)

if __name__ == '__main__':
    unittest.main()
