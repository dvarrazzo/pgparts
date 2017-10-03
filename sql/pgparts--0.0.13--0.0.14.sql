create or replace function
archive_partition(part regclass) returns void
language plpgsql as
$$
declare
    state @extschema@.partition_state = @extschema@._partition_state(part);
    archive regclass;
    parent regclass;
begin
    if state is null or state != 'present' then
        raise object_not_in_prerequisite_state using
            message = format(
                'The table %s is not an active partition', part);
    end if;

    select base_table from @extschema@.existing_partition
        where partition = part
        into strict parent;

    archive = @extschema@._archive_table(parent);
    if archive is null then
        raise undefined_table using
            message = format('archive table for %s not found', parent),
            hint = format(
                'You should run "@extschema@.create_archive(%s)" before.',
                parent);
    end if;

    raise notice 'archiving partition %', part;
    perform @extschema@._no_inherit(part, parent);
    perform @extschema@._inherit(part, archive);
    perform @extschema@.maintain_insert_function(parent);
end
$$;

create or replace function
unarchive_partition(part regclass) returns regclass
language plpgsql as
$$
declare
    state @extschema@.partition_state = @extschema@._partition_state(part);
    archive regclass;
    parent regclass;
begin
    if state is null or state != 'archived' then
        raise object_not_in_prerequisite_state using
            message = format(
                'The table %s is not an archived partition', part);
    end if;

    select @extschema@._parents(part) into strict archive;
    select base_table from @extschema@.existing_partition p
        where p.partition = part
        into strict parent;

    raise notice 'unarchiving partition %', part;
    perform @extschema@._no_inherit(part, archive);
    perform @extschema@._inherit(part, parent);

    perform @extschema@.maintain_insert_function(parent);
    return part;
end
$$;
