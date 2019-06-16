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
    def __init__(self, adapter, table_name, key, attributes=None):
        self.adapter = adapter
        self.table_name = table_name
        self.keys = key
        self.__json = { }

        if not attributes:
            # Fetching data
            self.adapter.get_item(table_name, key)
            # logger.info('fetching: %s' % str(self.keys))

            # try:
            #     response = self.table.client.get_item(
            #         TableName=self.table.table_name,
            #         Key=self.keys
            #     )
            #     if 'Item' in response:
            #         self.__json = UNFLUFF(response)
            #     else:
            #         raise KeyError("Record doesn't exist with key: %s" % str(key))
            # except self.table.client.exceptions.ResourceNotFoundException as e:
            #     # botocore.exceptions.ClientError
            #     logger.error(e)
            #     logger.info(self.table.table_name)
            #     raise KeyError(str(e))
        else:
            # Inserting data
            self.adapter.put_item(table_name, key, attributes)
            ## Migrate
            # logger.info('inserting: {%s : %s}' % (str(self.keys), attributes))
            # items = {
            #     attribute_name:
            #     {
            #         DYNAMODB_DATATYPES_LOOKUP[type(attribute_value).__name__]: str(attribute_value)
            #     } for attribute_name, attribute_value in attributes.items()
            # }
            # items = {**items, **self.keys}
            # try:
            #     self.describe = self.table.client.put_item(
            #         TableName=self.table.table_name,
            #         Item=items
            #     )
            # except botocore.exceptions.ClientError as e:
            #     logger.error(e)
            #     raise KeyError(str(e))


    # def __new__(cls, table, key, attributes=None):
    #     if attributes:
    #         return super(DynoRecord, cls).__new__(cls)
    #     else:
    #          pass


    def __getitem__(self, key):
        """
        """
        return self.adapter.get_item(self.table_name, key)
        ## Migrate
        # try:
        #     logger.info('getitem: %s' % str(key))
        #     return self.__json[key]
        # except KeyError:
        #     return self.__json


    def __setitem__(self, key, attributes):
        """
        """
        self.adapter.update_item(self.table_name, key, attributes)
        ## Migrate
        # logger.info('setitem: %s - %s - %s' % (self.__json, str(key), attributes))
        # self.table.client.update_item(
        #     TableName=self.table.table_name,
        #     Key=self.keys,
        #     ExpressionAttributeNames={
        #         '#X': str(key)
        #     },
        #     ExpressionAttributeValues={
        #         ':y': {
        #             DYNAMODB_DATATYPES_LOOKUP[type(attributes).__name__]: str(attributes),
        #         },
        #     },
        #     UpdateExpression='SET #X = :y',
        # )


    @property
    def json(self):
        return self.__json

