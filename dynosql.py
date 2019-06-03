#!env/bin/python3

import botocore
import botocore.session
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s:%(name)s:%(lineno)s:%(levelname)s - %(message)s",
                    level="INFO")

# https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes
# partition key can only be (S, N, B)

""" Convert python datatypes into to dynamodb datatypes
"""
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

DYNAMODB_DATATYPES_LOOKUP2 = {
   'S': 'str', # Scalar Types
   # binary - TODO
   'M': 'dict', # Document Types
   'L': 'list',
   'N': 'int',
   # Set Types - TODO
   # string set, number set, and binary set.
}

def DYNAMODB_DATATYPES_REVERSE_LOOKUP(db_type, value=None):
    """ Convert dynamodb datatypes into python datatypes
    """
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


def UNFLUFF(fluff):
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


class DynoRecord(object):
    """ DynoRecord is the wrapper class around each record

    """
    def __init__(self, table, key, attributes):
        self.table = table
        self.key = key

        items = {
            attribute_name:
            {
                DYNAMODB_DATATYPES_LOOKUP[type(attribute_value).__name__]: str(attribute_value)
            } for attribute_name, attribute_value in attributes.items()
        }

        try:
            partition_key_value, sort_key_value = self.key
            items[self.table.partition_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.table.partition_key[1]]: str(partition_key_value) }
            items[self.table.sort_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.table.sort_key[1]]: str(sort_key_value) }
        except ValueError:
            partition_key_value, sort_key_value = (self.key, None,)
            items[self.table.partition_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.table.partition_key[1]]: str(partition_key_value) }
        
        logger.info(partition_key_value)
        logger.info(table.partition_key)
        logger.info(items)

        try:
            response = self.table.client.put_item(
                TableName=self.table.table_name,
                Item=items
            )
            self.record = response
        except botocore.exceptions.ClientError as e:
            logger.error(e)
            raise KeyError(str(e))
        

    def __getitem__(self, key):
        """
        """
        logger.info(key)
        try:
            partition_key_value, sort_key_value = self.key
            keys = {
                self.table.partition_key[0]: { DYNAMODB_DATATYPES_LOOKUP[self.table.partition_key[1]]: partition_key_value },
                self.table.sort_key[0]: { DYNAMODB_DATATYPES_LOOKUP[self.table.sort_key[1]]: sort_key_value }
            }
        except ValueError:
            partition_key_value, sort_key_value = (self.key, None,)
            keys = {
                self.table.partition_key[0]: { DYNAMODB_DATATYPES_LOOKUP[self.table.partition_key[1]]: partition_key_value }
            }
        except TypeError:
            raise KeyError('Table was not defined with a sort key')

        try:
            response = self.table.client.get_item(
                TableName=self.table.table_name,
                Key=keys
            )
            response = UNFLUFF(response)
        except botocore.exceptions.ClientError as e:
            logger.error(e)
            raise KeyError(str(e))

        logger.info(response)
        return response


    def __setitem__(self, key, attributes):
        logger.info(key)
        logger.info(attributes)


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
    def __init__(self, client, table_name, partition_key=None, sort_key=None, **attributes):
        self.client = client
        self.table_name = table_name
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.descrbe = None
        self.record = {}

        KeySchema = []
        AttributeDefinitions = []

        if partition_key:
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
        logger.debug(KeySchema)
        logger.debug(AttributeDefinitions)

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
            description = self.client.describe_table(TableName=table_name)
            logger.debug(description['Table'])
            self.partition_key = (
                description['Table']['AttributeDefinitions'][0]['AttributeName'],
                DYNAMODB_DATATYPES_LOOKUP2[description['Table']['AttributeDefinitions'][0]['AttributeType']]
            )
            try:
                self.sort_key = (
                    description['Table']['AttributeDefinitions'][1]['AttributeName'],
                    DYNAMODB_DATATYPES_LOOKUP2[description['Table']['AttributeDefinitions'][1]['AttributeType']]
                )
            except IndexError:
                pass
            self.describe = description
        logger.debug(self.partition_key)
        logger.debug(self.sort_key)

    def drop(self):
        """ __del__ isn't very reliable in testing
            backup method for making sure tables are deleted

        Parameters:
        None

        Return:
        None
        """
        self.client.delete_table(TableName=self.table_name)
        logger.debug('deleted %s' % self.table_name)

    def __del__(self):
        """ Deletes the referenced table from DynamoDB

        Parameters:
        None

        Return:
        None
        """
        try:
            self.client.delete_table(TableName=self.table_name)
            logger.debug('deleted %s' % self.table_name)
        except ReferenceError:
            # This method is always called when the class is destroyed
            # if it is not called explicitly it will raise a ReferenceError
            pass
        except self.client.exceptions.ResourceNotFoundException:
            # table doesn't exist
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
        # items = {
        #     attribute_name:
        #     {
        #         DYNAMODB_DATATYPES_LOOKUP[type(attribute_value).__name__]: str(attribute_value)
        #     } for attribute_name, attribute_value in attributes.items()
        # }

        # try:
        #     partition_key_value, sort_key_value = key
        #     items[self.partition_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.partition_key[1]]: str(partition_key_value) }
        #     items[self.sort_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.sort_key[1]]: str(sort_key_value) }
        # except ValueError:
        #     partition_key_value, sort_key_value = (key, None,)
        #     items[self.partition_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.partition_key[1]]: str(partition_key_value) }
        
        # response = self.client.put_item(
        #     TableName=self.table_name,
        #     Item=items
        # )
        #logger.info(key)
        self.record[key] = DynoRecord(self, key, attributes)
        return self.record[key] # DynoRecord(self, key, attributes)

    def __getitem__(self, key):
        """ Retreive the record from DynamoDB based on the passed key

        Parameters::
        key (tuple/string): can be a tuple if a composite key is used as primary key
            otherwise a string containing the partition key

        Return:
        dict: Returns record from DynamoDB
        """
        logger.info(self.partition_key)
        logger.info(DYNAMODB_DATATYPES_LOOKUP[self.partition_key[1]])
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
        except TypeError:
            raise KeyError('Table was not defined with a sort key')

        try:
            response = self.client.get_item(
                TableName=self.table_name,
                Key=keys
            )
            response = UNFLUFF(response)
        except botocore.exceptions.ClientError as e:
            logger.error(e)
            raise KeyError(str(e))

        logger.info(response)
        logger.info(key)

        self.record[key].last_query = response

        return response


class Dynosql(object):
    """ Base class for Dyno project initiates a session with DynamoDB then
        through the call method creates a table reference
    """

    def __init__(self, endpoint_url='http://localhost:8000/'):
        session = botocore.session.get_session()
        self.client = session.create_client('dynamodb', endpoint_url=endpoint_url)

    def __call__(self, table_name, partition_key=None, sort_key=None, **attributes):
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

    def keys(self):
        """ Fetches a list of table names from database

        Parameters:
        None

        Returns:
        list: of tablenames in database
        """
        return self.client.list_tables()['TableNames']


