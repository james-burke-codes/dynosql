import botocore
import botocore.session
import logging

logger = logging.getLogger(__name__)


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

# Functions: attribute_exists | attribute_not_exists | attribute_type | contains | begins_with | size These function names are case-sensitive.
# Comparison operators: = | <> | < | > | <= | >= | BETWEEN | IN
# Logical operators: AND | OR | NOT
DYNAMODB_EXPRESSION_LOOKUP = {
    '!=': '<>',
    '==': '=',
    '<': '<',
    '>': '>',
    '<=': '<=',
    '>=': '>='
}

ATTRIBUTE_VALUES = 'abcdefghijklmnopqrstuvwxyz'



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
    try:
        return [{
            k: DYNAMODB_DATATYPES_REVERSE_LOOKUP(
                list(v)[0],
                list(v.values())[0])(list(v.values())[0])
            for k, v in x.items()
        } for x in fluff['Items']]
    except KeyError:
        return {
            k: DYNAMODB_DATATYPES_REVERSE_LOOKUP(
                list(v)[0],
                list(v.values())[0])(list(v.values())[0])
            for k, v in fluff['Item'].items()
        }



class BotocoreAdapter(object):

    def __init__(self, endpoint_url='http://localhost:8000/'):
        session = botocore.session.get_session()
        self.client = session.create_client('dynamodb', endpoint_url=endpoint_url)
        self.tables = {}
        logger.info('initialised botocore...')


    def _get_keys(self, table_name, primary_key):
        """
        Returns {'artist': {'S': 'Michael Jackson'}}
        """
        logger.info(table_name)
        logger.info(self.tables)
        try:
            partition_key_value, sort_key_value = primary_key
            keys = {
                self.tables[table_name]['partition_key'][0]: {
                    DYNAMODB_DATATYPES_LOOKUP[self.tables[table_name]['partition_key'][1]]: str(partition_key_value)
                },
                self.tables[table_name]['sort_key'][0]: {
                    DYNAMODB_DATATYPES_LOOKUP[self.tables[table_name]['sort_key'][1]]: str(sort_key_value)
                }
            }
        except ValueError:
            partition_key_value, sort_key_value = (primary_key, None,)
            keys = {
                self.tables[table_name]['partition_key'][0]: {
                    DYNAMODB_DATATYPES_LOOKUP[self.tables[table_name]['partition_key'][1]]: str(partition_key_value)
                }
            }
        except TypeError:
            raise KeyError('Table was not defined with a sort key')
        logger.info(keys)
        return keys


    def _define_primary_key(self, table_name, description):
        #'KeySchema': [{'AttributeName': 'song', 'KeyType': 'HASH'}]
        #'AttributeDefinitions': [{'AttributeName': 'song', 'AttributeType': 'S'}]
        #
        keys = {}
        for i, keyschema in enumerate(description['Table']['KeySchema']):
            definition = description['Table']['AttributeDefinitions'][i]
            if keyschema['KeyType'] == 'HASH':
                self.tables[table_name]['partition_key'] = (keyschema['AttributeName'], DYNAMODB_DATATYPES_LOOKUP2[definition['AttributeType']])
            else:
                self.tables[table_name]['sort_key'] = (keyschema['AttributeName'], DYNAMODB_DATATYPES_LOOKUP2[definition['AttributeType']])


    def list_tables(self):
        table_list = self.client.list_tables()
        logger.info(table_list)
        return table_list


    def create_table(self, table_name, partition_key=None, sort_key=None, **attributes):
        logger.info(partition_key)
        logger.info(sort_key)
        KeySchema = []
        AttributeDefinitions = []
        self.tables[table_name] = {}
        #     'partition_key': partition_key,
        #     'sort_key': sort_key
        # }

        if partition_key:
            self.tables[table_name]['partition_key'] = partition_key
            KeySchema.append({ 'AttributeName': partition_key[0], 'KeyType': 'HASH' })
            AttributeDefinitions.append(
                {
                    'AttributeName': partition_key[0],
                    'AttributeType': DYNAMODB_DATATYPES_LOOKUP[partition_key[1]]
                }
            )
        if sort_key:
            self.tables[table_name]['sort_key'] = sort_key
            KeySchema.append({ 'AttributeName': sort_key[0], 'KeyType': 'RANGE' })
            AttributeDefinitions.append(
                {
                    'AttributeName': sort_key[0],
                    'AttributeType': DYNAMODB_DATATYPES_LOOKUP[sort_key[1]]
                }
            )
        logger.info(KeySchema)
        logger.info(AttributeDefinitions)

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
            return description
        except botocore.exceptions.ParamValidationError as e:
            description = self.client.describe_table(TableName=table_name)
            logger.info(description['Table'])
            partition_key = (
                description['Table']['AttributeDefinitions'][0]['AttributeName'],
                DYNAMODB_DATATYPES_LOOKUP2[description['Table']['AttributeDefinitions'][0]['AttributeType']]
            )
            try:
                sort_key = (
                    description['Table']['AttributeDefinitions'][1]['AttributeName'],
                    DYNAMODB_DATATYPES_LOOKUP2[description['Table']['AttributeDefinitions'][1]['AttributeType']]
                )
            except IndexError:
                pass

            self._define_primary_key(table_name, description)
            return description


    def delete_table(self, table_name):
        try:
            self.client.delete_table(TableName=table_name)
            logger.debug('deleted %s' % table_name)
        except ReferenceError:
            # This method is always called when the class is destroyed
            # if it is not called explicitly it will raise a ReferenceError
            pass
        except self.client.exceptions.ResourceNotFoundException:
            # table doesn't exist
            pass


    def get_item(self, table_name, primary_key):
        # WIP - figure out key with _get_keys function
        logger.info('fetching: %s' % str(primary_key))
        keys = self._get_keys(table_name, primary_key)
        logger.info('_get_keys: %s' % str(keys))
        try:
            response = self.client.get_item(
                TableName=table_name,
                Key=keys
            )
            if 'Item' in response:
                return UNFLUFF(response)
            else:
                raise KeyError("Record doesn't exist with key: %s" % str(primary_key))
        except self.client.exceptions.ResourceNotFoundException as e:
            # botocore.exceptions.ClientError
            logger.error(e)
            logger.info(table_name)
            raise KeyError(str(e))


    def put_item(self, table_name, primary_key, attributes):
        # WIP - figure out key with _get_keys function
        logger.info('inserting: {%s : %s}' % (str(primary_key), attributes))
        items = {
            attribute_name:
            {
                DYNAMODB_DATATYPES_LOOKUP[type(attribute_value).__name__]: str(attribute_value)
            } for attribute_name, attribute_value in attributes.items()
        }
        items = {**items, **self._get_keys(table_name, primary_key)}
        try:
            self.describe = self.client.put_item(
                TableName=table_name,
                Item=items
            )
        except botocore.exceptions.ClientError as e:
            logger.error(e)
            raise KeyError(str(e))

    def update_item(self, table_name, primary_key, key, value):
        logger.info('setitem: %s - %s' % (str(key), value))
        self.client.update_item(
            TableName=table_name,
            Key=self._get_keys(table_name, primary_key),
            ExpressionAttributeNames={
                '#X': str(key)
            },
            ExpressionAttributeValues={
                ':y': {
                    DYNAMODB_DATATYPES_LOOKUP[type(value).__name__]: str(value),
                },
            },
            UpdateExpression='SET #X = :y',
        )


    def delete_item(self, table_name, primary_key):
        self.client.delete_item(
            TableName=table_name,
            Key=self._get_keys(table_name, primary_key)
        )

    def filter(self, table_name, filter_expression):
        import inspect
        import re

        #logger.info(self.queries)
        logger.info(filter_expression)

        exp_attribute, exp_operator, exp_value = filter_expression

        filter_expression_values = "{} {} :{}".format(
            exp_attribute,
            exp_operator,
            ATTRIBUTE_VALUES[0]
        )

        expression_attribute_values = {
            ':{}'.format(ATTRIBUTE_VALUES[0]): {
                DYNAMODB_DATATYPES_LOOKUP[type(exp_value).__name__]: str(exp_value)
            }
        }

        logger.info(filter_expression_values)
        logger.info(expression_attribute_values)

        response =  self.client.scan(
            TableName=table_name,
            ExpressionAttributeValues=expression_attribute_values,
            FilterExpression=filter_expression_values
        )
        logger.info(response)
        logger.info(UNFLUFF(response))

        return UNFLUFF(response)



