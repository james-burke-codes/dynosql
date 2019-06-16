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
        self.assertNotIn('music', self.dyno.list_tables())


    def test_insert_record_with_sort_key(self):
        self.tables['music'] = self.dyno(table_name='music', partition_key=('artist', 'str',), sort_key=('song', 'str',))
        self.tables['music']['Prince', 'Purple Rain'] = {'released': 1984, 'album': 'Purple Rain'}
        self.assertEqual(self.tables['music']['Prince', 'Purple Rain']['released'], 1984)

    # def test_insert_record_without_sort_key(self):
    #     self.tables['music'] = self.dyno(table_name='music', partition_key=('song', 'str',))
    #     self.tables['music']['Prince - Purple Rain'] = {'released': 1984, 'album': 'Purple Rain'}
    #     self.assertEqual(self.tables['music']['Prince - Purple Rain']['released'], 1984)

    # def test_insert_record_with_sort_key_but_retreive_without(self):
    #     self.tables['music'] = self.dyno(table_name='music', partition_key=('artist', 'str',), sort_key=('song', 'str',))
    #     self.tables['music']['Prince', 'Purple Rain'] = {'released': 1984, 'album': 'Purple Rain'}
    #     self.assertRaises(KeyError)

    # def test_insert_record_without_sort_key_but_retreive_with(self):
    #     self.tables['music'] = self.dyno(table_name='music', partition_key=('song', 'str',))
    #     self.tables['music']['Prince - Purple Rain'] = {'released': 1984, 'album': 'Purple Rain'}
    #     with self.assertRaises(KeyError):
    #         print(self.tables['music']['Prince, Purple Rain'])


    # def test_reference_existing_table(self):
    #     self.tables['music'] = self.dyno(table_name='music', partition_key=('song', 'str',))
    #     self.tables['music']['Prince - Purple Rain'] = {'released': 1984, 'album': 'Purple Rain'}

    #     self.tables['music2'] = self.dyno(table_name='music')
    #     self.assertEqual(self.tables['music2']['Prince - Purple Rain']['released'], 1984)

    # def test_update_record_argument(self):
    #     self.tables['music'] = self.dyno(table_name='music', partition_key=('artist', 'str',), sort_key=('song', 'str',))
    #     self.tables['music']['Prince', 'Purple Rain'] = {'released': 1983, 'album': 'Purple Rain'}
    #     self.tables['music']['Prince', 'Purple Rain']['released'] = 1984
    #     self.assertEqual(self.tables['music']['Prince', 'Purple Rain']['released'], 1984)


    # def test_delete_record(self):
    #     self.tables['music'] = self.dyno(table_name='music', partition_key=('artist', 'str',), sort_key=('song', 'str',))
    #     self.tables['music']['Prince', 'Purple Rain'] = {'released': 1983, 'album': 'Purple Rain'}
    #     self.tables['music']['Prince', 'Raspberry Beret'] = {'released': 1985, 'album': 'Around the World in a Day'}
    #     del self.tables['music']['Prince', 'Purple Rain']

    #     with self.assertRaises(KeyError):
    #         self.tables['music']['Prince', 'Purple Rain']


    # def test_filter_on_non_key(self):
    #     self.tables['music'] = self.dyno(table_name='music', partition_key=('artist', 'str',), sort_key=('song', 'str',))
    #     self.tables['music']['Prince', 'Purple Rain'] = {'released': 1983, 'album': 'Purple Rain'}
    #     self.tables['music']['Prince', 'Raspberry Beret'] = {'released': 1985, 'album': 'Around the World in a Day'}

    #     with self.subTest(name="equal comparison"):
    #         logger.info('Test: released == 1983')
    #         self.assertEqual(self.tables['music'].filter(
    #             self.tables['music'].released == 1983
    #         )[0]['album'], 'Purple Rain')

    # with self.subTest(name="equal comparison"):
    #     logger.info('Test: released == 1983 and released == 1984')
    #     self.assertEqual(self.tables['music'].filter(
    #         (self.tables['music'].released == 1983) & (self.tables['music'].released == 1984)
    #     )[0]['album'], 'Purple Rain')

    # with self.subTest(name="equal or comparison"):
    #     self.assertEqual(self.tables['music'].filter(
    #         lambda released: released == 1985 or released == 1983)[0]['album'],
    #         'Around the World in a Day'
    #     )

    # with self.subTest(name="not equal comparison"):
    #     self.assertEqual(self.tables['music'].filter(lambda released: released != 1985)[0]['album'], 'Purple Rain')

    # with self.subTest(name="no result"):
    #     logger.info(self.tables['music'].filter(lambda released: released > 1983))
    #     with self.assertRaises(IndexError):
    #         self.tables['music'].filter(lambda released: released == 1980)[0]['album']


if __name__ == '__main__':
    unittest.main()
