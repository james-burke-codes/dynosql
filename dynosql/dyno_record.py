import botocore
import logging

logger = logging.getLogger(__name__)

from dynosql.helper_methods import DYNAMODB_DATATYPES_LOOKUP
from dynosql.helper_methods import DYNAMODB_DATATYPES_LOOKUP2
from dynosql.helper_methods import DYNAMODB_DATATYPES_REVERSE_LOOKUP
from dynosql.helper_methods import UNFLUFF

class DynoRecord(object):
    """ DynoRecord is the wrapper class around each record

    """
    def __init__(self, adapter, table_name, primary_key, attributes=None):
        self.adapter = adapter
        self.table_name = table_name
        self.primary_key = primary_key
        self._json = { }

        if not attributes:
            # Fetching data
            self._json = self.adapter.get_item(table_name, primary_key)
        else:
            # Inserting data
            self.adapter.put_item(table_name, primary_key, attributes)


    # def __new__(cls, table, key, attributes=None):
    #     if attributes:
    #         return super(DynoRecord, cls).__new__(cls)
    #     else:
    #          pass


    def __getitem__(self, key):
        """
        """
        logger.info('getitem: %s' % str(key))
        try:
            return self._json[key]
        except KeyError:
            return self._json


    def __setitem__(self, key, attributes):
        """
        """
        logger.info('setitem: %s - %s' % (key, attributes))
        # logger.info(self.primary_key)
        self.adapter.update_item(self.table_name, self.primary_key, key, attributes)


    @property
    def json(self):
        return self._json

