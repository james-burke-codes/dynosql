# Dynosql

A wrapper for Amazon Web Services (AWS) DynamoDB to make interacting with DynamoDB more pythonic

## Current functions

```python
# initiate
import dynosql

dyno = dynosql.Dynosql()

# create table with composite key
music_ex1 = dyno(table_name='music', partition_key={'artist': 'str'}, sort_key={'song': 'str'})
music_ex1['White Stripes', 'Friends'] = {released=2002, album='White Blood Cells'}
music_ex1['White Stripes', 'Friends'].json


# define table with only a partition key
music_ex2 = dyno(table_name='music', partition_key={'song': 'str'})
music_ex2['White Stripes - Friends'] = {released=2002, album='White Blood Cells'}
music_ex2['White Stripes - Friends'].json


# reference existing table
music_ex3 = dyno(table_name='music')
music_ex3['White Stripes - Friends'].json


# delete table
del music_ex3
```
_Note: `music['White Stripes - Friends']` itself will return a DynoRecord object so you must use `.json` to get the record_

## Planned functions

```python
# update record
music['White Stripes', 'Friends']['released'] = 2001

# delete record
del music['White Stripes', 'Friends']

# select by condition on non-primary key attributes
music.filter(lambda released: released == 2002)
```