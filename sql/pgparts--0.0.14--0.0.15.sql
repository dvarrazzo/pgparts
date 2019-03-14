insert into schema_param values (
    'monthly', 'retention', 'interval', null,
$$The time a partition should be kept alive.

Unused by the extension itself but may be used by a pruning process.
$$);

insert into schema_param values (
    'daily', 'retention', 'interval', null,
$$The time a partition should be kept alive.

Unused by the extension itself but may be used by a pruning process.
$$);
