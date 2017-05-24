create or replace function
create_for("table" regclass, value text) returns regclass
language plpgsql as
$$
declare
    pname name;
    type regtype;
    info @extschema@.partition_info;
    partition regclass;
begin
    begin  -- transaction
        select (@extschema@.info("table", value)).* into strict info;
        if info.state = 'unpartitioned' then
            raise using
                message = format(
                    'the table %s has not been prepared for partitions yet',
                    "table"),
                hint = format('You should call @extschema@.setup(%L).',
                    "table");
        elsif info.state = 'present' then
            return info.partition;
        elsif info.state = 'detached' then
            raise using
                message = format('the partition %s exists but is detached',
                    info.partition),
                hint = format('You can attach it back using '
                        '@extschema@.attach_for(%L, %L).',
                    "table", value);
        elsif info.state = 'missing' then
            null;
        else
            raise 'unexpected partition state: %', info.state;
        end if;

        -- Not found: create it
        raise notice '%', format('creating partition %I.%I of table %s',
            @extschema@._schema_name("table"),
            @extschema@.name_for("table", value),
            "table");

        select @extschema@._copy_to_subtable("table", value)
        into strict partition;

        -- Insert the data about the partition in the table; the other
        -- functions will get the details from here
        select field_type from @extschema@.partitioned_table t
            where t."table" = create_for."table"
            into strict type;

        perform @extschema@._register_partition(
            @extschema@._schema_name(partition),
            @extschema@._table_name(partition),
            "table",
            @extschema@._cast(
                @extschema@.start_for("table", value),
                @extschema@._base_type(type)),
            @extschema@._cast(
                @extschema@.end_for("table", value),
                @extschema@._base_type(type)));

        perform @extschema@._constraint_partition(partition);
        if @extschema@._has_pkey("table") then
            perform @extschema@._create_partition_update_trigger(partition);
        end if;
        perform @extschema@.maintain_insert_function("table");

    exception
        -- you can't have this clause empty
        when division_by_zero then raise 'wat?';
    end;

    return partition;
end
$$;


create or replace function
detach_for("table" regclass, value text) returns regclass
language plpgsql as
$$
declare
    partition regclass = @extschema@.partition_for("table", value);
begin
    if partition is null then
        raise using
            message = format('there is no %I partition for %L',
                "table", value);
    end if;

    if partition in (select @extschema@._partitions("table")) then
        raise notice '%', format('detaching partition %s from %s',
            partition, "table");
        execute format('alter table %s no inherit %s',
            partition, "table");
        perform @extschema@.maintain_insert_function("table");
    end if;

    return partition;
end
$$;

create or replace function
attach_for("table" regclass, value text) returns regclass
language plpgsql as
$$
declare
    partition regclass = @extschema@.partition_for("table", value);
begin
    if partition is null then
        raise using
            message = format('there is no %I partition for %L',
                "table", value);
    end if;

    if partition not in (select @extschema@._partitions("table")) then
        raise notice '%', format('attaching partition %s to %s',
            partition, "table");
        execute format('alter table %s inherit %s',
            partition, "table");
        perform @extschema@.maintain_insert_function("table");
    end if;

    return partition;
end
$$;
