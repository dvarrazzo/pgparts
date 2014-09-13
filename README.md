pgparts -- minimal partitiions management
=========================================

The `pgarts` extension creates partitions on a table and install triggers to
maintain them. The base table is used as blueprint for the children table,
copying down all the indexes, constraints, and permission.

Installation is done by creating an extension, which can be created in any
schema. Following documentation will assume `parts`:

    CREATE SCHEMA parts;
    CREATE EXTENSION pgparts WITH SCHEMA parts;

You can set up a partitioned base table with the function:

    parts.setup(TABLE, FIELD, SCHEMA_NAME, SCHEMA_PARAMS);

where `SCHEMA_NAME` is one of the partitioning schemas available in the table
`parts.partition_schema` and `SCHEMA_PARAMS` the values required by such
schema. Check the `partition_schema.description` for help about the available
schemas.

Once the table is set up you can create a new partition for the table using:

    parts.create_for(TABLE, VALUE);

where `VALUE` is an example value for the `FIELD` previously set up: the
command creates the partition that will contain that value.
