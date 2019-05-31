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

    def __init__(self, client, table_name, partition_key, sort_key=None, **attributes):
        # name: value - type(value) = datatype
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
        try:
            self.client.delete_table(TableName=self.table_name)
            logger.warning('deleted %s' % self.table_name)
        except ReferenceError:
            pass

    def __setitem__(self, key, attributes):
        partition_key_value, sort_key_value = key
        items = {
            attribute_name: 
            {
                DYNAMODB_DATATYPES_LOOKUP[type(attribute_value).__name__]: str(attribute_value)
            } for attribute_name, attribute_value in attributes.items()
        }
        items[self.partition_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.partition_key[1]]: str(partition_key_value) }
        items[self.sort_key[0]] = { DYNAMODB_DATATYPES_LOOKUP[self.sort_key[1]]: str(sort_key_value) }
        logger.info(items)
        
        response = self.client.put_item(
            TableName=self.table_name,
            Item=items
        )
        logger.info(response)
        return response

    def __getitem__(self, key):
        partition_key_value, sort_key_value = key
        response = self.client.get_item(
            TableName=self.table_name,
            Key={
                self.partition_key[0]: { DYNAMODB_DATATYPES_LOOKUP[self.partition_key[1]]: partition_key_value },
                self.sort_key[0]: { DYNAMODB_DATATYPES_LOOKUP[self.sort_key[1]]: sort_key_value }
            }
        )
        return response


class Dyno(object):
    """
    Dict like object
    """
    session = botocore.session.get_session()

    def __init__(self, endpoint_url='http://localhost:8000/'):
        self.client =self.session.create_client('dynamodb', endpoint_url=endpoint_url)   

    def __call__(self, table_name, partition_key, sort_key=None, **attributes):
        return DynoTable(self.client, table_name, partition_key, sort_key, **attributes)


    # def __setitem__(self, key, item):
    #     if key in self.keys():
    #         logger.info('Table already exists')
    #         self.__dict__[key] = self.client.describe_table(TableName=key)
    #     else:
    #         item['TableName'] = key
    #         self.__dict__[key] = self.client.create_table(**item)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __delitem__(self, key):
        self.client.delete_table(TableName=key)
        del self.__dict__[key]

    def keys(self):
        return self.client.list_tables()['TableNames']


        # def create_record(self):
    #     items = {
    #         attribute_name: 
    #         {
    #             MAP_DATATYPES[type(attribute_value).__name__]: attribute_value
    #         } for attribute_name, attribute_value in attributes.items()
    #     }
    #     logger.info(items)
        
    #     self.client.put_item(
    #         TableName=key,
    #         Item=items
    #     )


if __name__ == '__main__':
    dyno = Dyno()
    logger.info(dyno.keys())

    music = dyno(table_name='music', partition_key={'artist': 'str'}, sort_key={'song': 'str'})
    del music
    # logger.info(music)
    # del music
    # logger.info(music)
