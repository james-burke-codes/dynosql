#!env/bin/python3

import botocore
import botocore.session
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s:%(name)s:%(lineno)s:%(levelname)s - %(message)s",
                    level="INFO")

# https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes
# partition key can only be (S, N, B)
DYNAMODB_DATATYPES_LOOKUP = {
   'str': 'S', # Scalar Types
   'int': 'N',
   'float': 'N',
   'long': 'N',
   # binary - TODO
   'dict': 'M', # Document Types
   'list': 'L'
   # Set Types - TODO
   # string set, number set, and binary set.
}

def DYNAMODB_DATATYPES_REVERSE_LOOKUP(db_type, value):
    lookup = {
       'S': str, # Scalar Types
       # binary - TODO
       'M': dict, # Document Types
       'L': list
       # Set Types - TODO
       # string set, number set, and binary set.
    }
    if db_type in ('S', 'M', 'L'):
        return lookup[db_type]
    elif db_type == 'N':
        if value.isdigit():
            return int
        else:
            try:
                float(value)
                return float
            except ValueError as e:
                return str

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

    def __init__(self, client, table_name, partition_key, sort_key=None, **attributes):
        self.client = client
        self.table_name = table_name
        self.partition_key = partition_key
        self.sort_key = sort_key

        KeySchema = []
        AttributeDefinitions = []
        #     {
        #         'AttributeName': attribute_name, 
        #         'AttributeType': DYNAMODB_DATATYPES_LOOKUP[type(attribute_value).__name__]
        #     }
        #     for attribute_name, attribute_value in attributes.items()
        # ]

        KeySchema.append({ 'AttributeName': partition_key[0], 'KeyType': 'HASH' })
        AttributeDefinitions.append(
            {
                'AttributeName': partition_key[0],
                'AttributeType': DYNAMODB_DATATYPES_LOOKUP[partition_key[1]]
            }
        )
        if sort_key:
            KeySchema.append({ 'AttributeName': sort_key[0], 'KeyType': 'RANGE' })
            AttributeDefinitions.append(
                {
                    'AttributeName': sort_key[0],
                    'AttributeType': DYNAMODB_DATATYPES_LOOKUP[sort_key[1]]
                }
            )

        try:
            description = self.client.create_table(
                TableName=table_name,
                KeySchema=KeySchema,
                AttributeDefinitions=AttributeDefinitions,
                ProvisionedThroughput={
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5,
                },
            )
            description['Table'] = description['TableDescription']
            del description['TableDescription']
            self.describe = description
        except Exception as e:
            logger.info(e)
            description = self.client.describe_table(TableName=table_name)
            self.describe = description

    def __del__(self):
        """ Deletes the referenced table from DynamoDB

        Parameters:
        None

        Return:
        None

        """
        try:
            self.client.delete_table(TableName=self.table_name)
            logger.info('deleted %s' % self.table_name)
        except ReferenceError:
            # This method is always called when the class is destroyed
            # if it is not called explicitly it will raise a ReferenceError
            pass

    def __setitem__(self, key, attributes):
        """ Inserts a new record or updates an existing one

        Parameters:
        key (tuple/string): can be a tuple if a composite key is used as primary key
            otherwise a string containing the partition key
        atttributes (dict): additional attributes

        Return:
        None: It is an assignment operator so cannot return a response

        """
        items = {
            attribute_name:
            {
                DYNAMODB_DATATYPES_LOOKUP[type(attribute_value).__name__]: str(attribute_value)
            } for attribute_name, attribute_value in attributes.items()
        }

        try:
            partition_key_value, sort_key_value = key
            items[self.partition_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.partition_key[1]]: str(partition_key_value) }
            items[self.sort_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.sort_key[1]]: str(sort_key_value) }
        except ValueError:
            partition_key_value, sort_key_value = (key, None,)
            items[self.partition_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.partition_key[1]]: str(partition_key_value) }
        
        response = self.client.put_item(
            TableName=self.table_name,
            Item=items
        )
        logger.info(response)

        return

    def __getitem__(self, key):
        """ Retreive the record from DynamoDB based on the passed key

        Parameters::
        key (tuple/string): can be a tuple if a composite key is used as primary key
            otherwise a string containing the partition key

        Return:
        dict: Returns record from DynamoDB

        """
        try:
            partition_key_value, sort_key_value = key
            keys = {
                self.partition_key[0]: { DYNAMODB_DATATYPES_LOOKUP[self.partition_key[1]]: partition_key_value },
                self.sort_key[0]: { DYNAMODB_DATATYPES_LOOKUP[self.sort_key[1]]: sort_key_value }
            }
        except ValueError:
            partition_key_value, sort_key_value = (key, None,)
            keys = {
                self.partition_key[0]: { DYNAMODB_DATATYPES_LOOKUP[self.partition_key[1]]: partition_key_value }
            }

        response = self.client.get_item(
            TableName=self.table_name,
            Key=keys
        )
        logger.info(response['Item'])
        response = self.__unfluff(response)
        logger.info(response)
        return response

    def __unfluff(self, fluff):
        """ Removes the

        Paramters:
        fluff (dict): dictionary value of record returned from DynamoDB

        Returns:
        dict: Returns unfluffed  response from DynamoDB

        """
        return {
            k: DYNAMODB_DATATYPES_REVERSE_LOOKUP(
                list(v)[0],
                list(v.values())[0])(list(v.values())[0])
            for k, v in fluff['Item'].items()
        }


class Dyno(object):
    """ Base class for Dyno project initiates a session with DynamoDB then
        through the call method creates a table reference

    """

    def __init__(self, endpoint_url='http://localhost:8000/'):
        session = botocore.session.get_session()
        self.client =self.session.create_client('dynamodb', endpoint_url=endpoint_url)

    def __call__(self, table_name, partition_key, sort_key=None, **attributes):
        """ After Dyno is initiated it can be called to create a table

        Parameters:
        table_name (string):
        partition_key (string):
        sort_key (string):
        attributes (dict):

        Returns:
        DynoTable: 

        """
        return DynoTable(self.client, table_name, partition_key, sort_key, **attributes)

    # def __delitem__(self, key):
    #     self.client.delete_table(TableName=key)
    #     del self.__dict__[key]

    # def keys(self):
    #     return self.client.list_tables()['TableNames']

