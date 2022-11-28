# Data types problem

During batch processing we get type of column from the first message where column appears.
If latter messages have incompatible type for the same column - we fail to process entire batch.

1. We need be more smart when selection column types for tmp tables.
2. For existing columns we need to know about types incompatibility in specific message before trying to load entire batch.
That way we can skip or workaround incompatible messages and successfully load rest of a batch.

# Solution

## Part1 – For new columns get the lowest common ancestor type

During accumulation of batch we should keep track of all value types per each new column.

We can detect the lowest common ancestor type for the column.
For example, if we have `INTEGER` and `FLOAT` - we use `FLOAT` for the column.
It is not hard to do using `DataType` abstraction

## Part2 – For existing columns check that data is castable to the column type

When destination table already exists we can check for each messages in a batch that all values are castable to the corresponding column types.

If data is not castable we can skip or workaround the message.

If data is castable we can explicitly cast it to the column type or rely on database implicit cast.

### Castability check

We can't check castability using `DataType` abstraction because when we get `Table` schema from DB we loose information about `DataType` of each column.
We need to be able to restore `DataType` information for table from in `GetTableSchema`
Alternatively we can use `reflect.Type` but also need to enrich `Table` columns information with `reflect.Type` and build castability tree for `reflect.Type`

## Part3 – Workaround for not castable data types

We can still load events with not castable data types:
 * We need to remove problematic fields from such events.
 * We need to add `_unmaped_data` field of `JSON` type to such events that will contain object with problematic fields and its original values.
That will allow user to map problematic fields to the destination table columns manually.

