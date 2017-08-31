create or replace function
_too_old_predicate("table" regclass, prefix text default '') returns text
language sql stable as
$f$
    select format('%s%I < %L::%I',
        prefix, t.field, min(p.start_value), typname)
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on t."table" = p.base_table
    join pg_type on t.field_type = pg_type.oid
    where t."table" = $1
    and p.partition in (select @extschema@._partitions("table"))
    group by prefix, t.field, typname
$f$;
