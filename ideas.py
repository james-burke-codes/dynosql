# initiate
dyno = Dyno()

# create table
music = dyno(table_name='music', partition_key={'artist': 'str'}, sort_key={'song': 'str'})

# insert record
music['White Stripes', 'Friends'] = {released=2002, album='White Blood Cells'}

# select by composite key
music['White Stripes', 'Friends']

# select by partition key
music['White Stripes - Friends']

# select by condition
music.filter(lambda released: released == 2002)

# update record
music['White Stripes', 'Friends']['released'] = 2001

# delete record
del music['White Stripes', 'Friends']

# delete table
del music

# initiate
# dyno = DynoClient()

# logger.info('---- create test table ----')
# dyno['test'] = dyno(partition_key={'pk': 'str'}, sort_key={'sk': 'str'}, attr1='str', attr2='int')
# logger.info(dyno.keys())

# logger.info('----- show test table -----')
# logger.info(dyno['test'])

# logger.info(dyno['test'])

# logger.info('---- delete test table ----')
# del dyno['test']

# logger.info('----- show tables')
# logger.info(dyno.keys())


# AttributeDefinitions=[
#         {
#             'AttributeName': 'Artist',
#             'AttributeType': 'S',
#         },
#         {
#             'AttributeName': 'SongTitle',
#             'AttributeType': 'S',
#         },
#     ],
#     KeySchema=[
#         {
#             'AttributeName': 'Artist',
#             'KeyType': 'HASH',
#         },
#         {
#             'AttributeName': 'SongTitle',
#             'KeyType': 'RANGE',
#         },
#     ],
#     ProvisionedThroughput={
#         'ReadCapacityUnits': 5,
#         'WriteCapacityUnits': 5,
#     },
#     TableName='Music',

# # design

# # create table
# dyno['tablename'] = dyno(pk=1, sk=2, **others)

# # select by ids
# dyno['tablename']['partition_key']
# number_of_read_lengths[14,3] = "Your value"
# dyno['tablename']['partition_key', 'sort_key']


# class Mapping(dict):

#     def __setitem__(self, key, item):
#         self.__dict__[key] = item

#     def __getitem__(self, key):
#         return self.__dict__[key]

#     def __repr__(self):
#         return repr(self.__dict__)

#     def __len__(self):
#         return len(self.__dict__)

#     def __delitem__(self, key):
#         del self.__dict__[key]

#     def clear(self):
#         return self.__dict__.clear()

#     def copy(self):
#         return self.__dict__.copy()

#     def has_key(self, k):
#         return k in self.__dict__

#     def update(self, *args, **kwargs):
#         return self.__dict__.update(*args, **kwargs)

#     def keys(self):
#         return self.__dict__.keys()

#     def values(self):
#         return self.__dict__.values()

#     def items(self):
#         return self.__dict__.items()

#     def pop(self, *args):
#         return self.__dict__.pop(*args)

#     def __cmp__(self, dict_):
#         return self.__cmp__(self.__dict__, dict_)

#     def __contains__(self, item):
#         return item in self.__dict__

#     def __iter__(self):
#         return iter(self.__dict__)

#     def __unicode__(self):
#         return unicode(repr(self.__dict__))

class DynoTable(object):
    """
    List like object
    Can also reference records like a dict when using primary key/s
    """
    def __init__(self):
        """
        Constructs the call to boto3.create_table the actual call is handled in
        DynoClient.__setitem__
        """
        pass        



# o = Mapping()
# o.foo = "bar"
# o['lumberjack'] = 'foo'
# o.update({'a': 'b'}, c=44)
# print 'lumberjack' in o
# print o

# In [187]: run mapping.py
# True
# {'a': 'b', 'lumberjack': 'foo', 'foo': 'bar', 'c': 44}