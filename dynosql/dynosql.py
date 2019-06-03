#!env/bin/python3

import botocore
import botocore.session
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)s:%(name)s:%(lineno)s:%(levelname)s - %(message)s",
                    level="INFO")

from dynosql.dyno_table import DynoTable


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


