# Dynosql

A wrapper for Amazon Web Services (AWS) DynamoDB to make interacting with DynamoDB more pythonic

## Current features

```python
# initiate
import dynosql

dyno = dynosql.Dynosql()

# create table with composite key
music_ex1 = dyno(table_name='music', partition_key={'artist': 'str'}, sort_key={'song': 'str'})
music_ex1['White Stripes', 'Friends'] = { released: 2002, album: 'White Blood Cells' }
music_ex1['White Stripes', 'Friends'].json


# define table with only a partition key
music_ex2 = dyno(table_name='music', partition_key={'song': 'str'})
music_ex2['White Stripes - Friends'] = { released: 2002, album: 'White Blood Cells' }
music_ex2['White Stripes - Friends'].json


# reference existing table
music_ex3 = dyno(table_name='music')
music_ex3['White Stripes - Friends'].json


# update record attribute
music['White Stripes', 'Friends']['released'] = 2001


# delete table
del music_ex3
# or
music_ex3.drop()


# delete record
del music['White Stripes', 'Friends']


# select by condition on non-primary key attributes
music.filter(lambda released: released == 2002)
```
_Note: `music['White Stripes - Friends']` itself will return a DynoRecord object so you must use `.json` to get the record_

## Planned features

```python
# bulk insert
music.extend(
    { artist: 'White Stripes', song: 'Friends', released: 2002, album: 'White Blood Cells'},
    { artist: 'White Stripes', song: 'Ichy Thumb', released: 2007, album: 'Icky Thump'}
)

# add support for data all types MAP, LIST, BYTES, etc
```