import logging

logger = logging.getLogger(__name__)

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
