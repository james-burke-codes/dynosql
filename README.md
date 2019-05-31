# DynamoDB

A wrapper for botocore to make interacting with DynamoDB more pythonic

## Current functions

```python
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

# update record
music['White Stripes', 'Friends']['released'] = 2001

# delete record
del music['White Stripes', 'Friends']

# delete table
del music
```

## Planned functions

```python
# select by condition on non-primary key attributes
music.filter(lambda released: released == 2002)
```