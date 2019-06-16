import botocore
import logging

logger = logging.getLogger(__name__)

from dynosql.dyno_record import DynoRecord
from dynosql.dyno_attribute import DynoAttribute

from dynosql.helper_methods import DYNAMODB_DATATYPES_LOOKUP
from dynosql.helper_methods import DYNAMODB_DATATYPES_LOOKUP2
from dynosql.helper_methods import DYNAMODB_DATATYPES_REVERSE_LOOKUP
from dynosql.helper_methods import UNFLUFF
from dynosql.helper_methods import ATTRIBUTE_VALUES, DYNAMODB_EXPRESSION_LOOKUP


class DynoTable(object):
    """ DynoTable is a wrapper class around botocore. Each instance references a table in DynamoDB
        and mimics the behaviour python dict for simple opperations such as:
         * inserting records
         * retrieving records by id
         * deleting records

        More advanced features are handled more like list functions:
         * filtering by non primary key attributes
         * bulk inserting or updates

    """
    def __init__(self, adapter, table_name, partition_key=None, sort_key=None, **attributes):
        self.adapter = adapter
        self.table_name = table_name
        # self.partition_key = partition_key
        # self.sort_key = sort_key
        self.__info = None
        self.queries = []
        logger.info(partition_key)
        logger.info(sort_key)
        self.__info = self.adapter.create_table(table_name=table_name, partition_key=partition_key, sort_key=sort_key, attributes=attributes)


    def __setitem__(self, primary_key, attributes):
        """ Inserts a new record or replaces an existing one

        Parameters:
        primary_key (tuple/string): can be a tuple if a composite key is used as primary key
            otherwise a string containing the partition key
        atttributes (dict): additional attributes

        Return:
        None: It is an assignment operator so cannot return a response
        """
        logger.info('setitem: %s - %s' % (primary_key, attributes))
        DynoRecord(self.adapter, self.table_name, primary_key, attributes)


    def __getitem__(self, primary_key):
        """ Retreive record with key

        Parameters:
        primary_key (tuple/string): composite/primary key for the record
            otherwise a string containing the partition key

        Return:
        dict: Returns record from DynamoDB
        """
        logger.info('getitem: %s' % str(primary_key))
        return DynoRecord(self.adapter, self.table_name, primary_key)


    def __delitem__(self, primary_key):
        """ Delete record from a table

        Parameters:
        primary_key (string/tuple): composite/primary key for the record
        """
        logger.info('delete: %s' % str(primary_key))
        self.adapter.delete_item(self.table_name, primary_key)


    def __del__(self):
        """ Deletes the referenced table from database

        Parameters:
        None

        Return:
        None
        """
        self.adapter.delete_table(self.table_name)


    @property
    def info(self):
        return self.__info


    def __getattr__(self, name):
        logger.info(name)
        #self.queries.append(DynoAttribute(name))
        return DynoAttribute(name) #self.queries[-1]

    def __setattr__(self, name, value):
        logger.info(name)
        logger.info(value)
        super(DynoTable, self).__setattr__(name, value)


    def filter(self, filter_expression):
        # logger.info(self.queries)
        logger.info(filter_expression)
        return self.adapter.filter(self.table_name, filter_expression)


    def drop(self):
        """ __del__ isn't very reliable in testing
            backup method for making sure tables are deleted

        Parameters:
        None

        Return:
        None
        """
        self.adapter.delete_table(self.table_name)

