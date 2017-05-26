create or replace function
_value2key(field_type regtype, schema_name name, params params, value text)
returns text language plpgsql stable as $$
declare
    value2key text;
    rv text;
begin
    select v.value2key from @extschema@._schema_vtable v
    where (v.field_type, v.schema_name)
        = (_value2key.field_type, _value2key.schema_name)
    into strict value2key;

    execute format('select %s($1, $2::%s)', value2key, field_type)
    into strict rv using params, value;
    return rv;
end
$$;

create or replace function
_value2name(
    field_type regtype, schema_name name, params params,
    value text, base_name name)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2name text;
    rv text;
begin
    select v.value2key, v.key2name from @extschema@._schema_vtable v
    where (v.field_type, v.schema_name)
        = (_value2name.field_type, _value2name.schema_name)
    into strict value2key, key2name;

    execute format('select %s($1, %s($1, $2::%s))',
        key2name, value2key, @extschema@._base_type(field_type))
    into strict rv using params, value;
    return base_name || '_' || rv;
end
$$;

create function
_values2name(
    field_type regtype, schema_name name, params params,
    start_value text, end_value text, base_name name)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2name text;
    p1 text;
    p2 text;
begin
    select v.value2key, v.key2name from @extschema@._schema_vtable v
    where (v.field_type, v.schema_name)
        = (_values2name.field_type, _values2name.schema_name)
    into strict value2key, key2name;

    execute format(
        'select %1$s($1, %2$s($1, $2::%3$s)), %1$s($1, %2$s($1, $3::%3$s))',
        key2name, value2key, @extschema@._base_type(field_type))
    into strict p1, p2 using params, start_value, end_value;

    return base_name || '_' || p1 || '_' || p2;
end
$$;

create or replace function
_value2start(field_type regtype, schema_name name, params params, value text)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2start text;
    rv text;
begin
    select v.value2key, v.key2start from @extschema@._schema_vtable v
    where (v.field_type, v.schema_name)
        = (_value2start.field_type, _value2start.schema_name)
    into strict value2key, key2start;

    execute format('select %s($1, %s($1, $2::%s))',
        key2start, value2key, @extschema@._base_type(field_type))
    into strict rv using params, value;
    return rv;
end
$$;

create or replace function
_value2end(field_type regtype, schema_name name, params params, value text)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2end text;
    rv text;
begin
    select v.value2key, v.key2end from @extschema@._schema_vtable v
    where (v.field_type, v.schema_name)
        = (_value2end.field_type, _value2end.schema_name)
    into strict value2key, key2end;

    execute format('select %s($1, %s($1, $2::%s))',
        key2end, value2key, @extschema@._base_type(field_type))
    into strict rv using params, value;
    return rv;
end
$$;

create function
_name_for("table" regclass, start_value text, end_value text) returns name
language sql stable as
$$
    select @extschema@._values2name(
        cfg.field_type, cfg.schema_name, cfg.schema_params,
        start_value, end_value, relname)
    from pg_class r
    join @extschema@.partitioned_table cfg on r.oid = cfg."table"
    where cfg."table" = _name_for."table";
$$;

create or replace function
partition_for("table" regclass, value text) returns regclass
language plpgsql as
$f$
declare
    rv regclass;
begin
    execute format($$
        select p.partition from @extschema@.existing_partition p
        where base_table = $1
        and p.start_value::%1$s <= $2::%1$s
        and $2::%1$s < p.end_value::%1$s
        $$, @extschema@._base_type(@extschema@._partition_field_type("table")))
    into rv
    using "table", value;
    return rv;
end
$f$;

create function
_overlapping("table" regclass, start_value text, end_value text)
returns regclass[] language plpgsql as
$f$
declare
    rv regclass[];
begin
    execute format($$
        select array_agg(p.partition) from (
            select * from @extschema@.existing_partition p
            where base_table = $1
            and p.start_value::%1$s < $3::%1$s
            and $2::%1$s < p.end_value::%1$s
            order by p.partition::text) p
        $$, @extschema@._base_type(@extschema@._partition_field_type("table")))
    into rv
    using "table", start_value, end_value;
    return rv;
end
$f$;


create or replace function
create_for("table" regclass, value text) returns regclass
language plpgsql as
$$
declare
    pname name;
    type regtype;
    info @extschema@.partition_info;
    partition regclass;
    start_value text;
    end_value text;
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
        elsif info.state = 'archived' then
            raise using
                message = format('the partition %s exists but was archived',
                    info.partition),
                hint = format('You can re-enable it using '
                        '@extschema@.unarchive(%L).',
                    info.partition);
        elsif info.state = 'missing' then
            null;
        else
            raise 'unexpected partition state: %', info.state;
        end if;

        select field_type from @extschema@.partitioned_table t
            where t."table" = create_for."table"
            into strict type;
        start_value = @extschema@._cast(
            @extschema@.start_for("table", value),
            @extschema@._base_type(type));
        end_value = @extschema@._cast(
            @extschema@.end_for("table", value),
            @extschema@._base_type(type));

        if array_length(@extschema@._overlapping(
                "table", start_value, end_value), 1) > 0 then
            raise 'the partition(s) % overlap the range requested',
                array_to_string(@extschema@._overlapping(
                    "table", start_value, end_value), ', ');
        end if;

        -- Not found: create it
        raise notice '%', format('creating partition %I.%I of table %s',
            @extschema@._schema_name("table"),
            @extschema@.name_for("table", value),
            "table");

        partition = @extschema@._copy_to_subtable("table",
            @extschema@.name_for("table", value));

        -- Insert the data about the partition in the table; the other
        -- functions will get the details from here
        perform @extschema@._register_partition(
            @extschema@._schema_name(partition),
            @extschema@._table_name(partition),
            "table", start_value, end_value);

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


create function
create_partition("table" regclass, start_value text, end_value text) returns regclass
language plpgsql as
$$
declare
    pname name;
    type regtype;
    info @extschema@.partition_info;
    partition regclass;
begin
    begin  -- transaction
        perform 1 from @extschema@.partitioned_table pt
        where pt."table" = create_partition.table;
        if not found then
            raise using
                message = format(
                    'the table %s has not been prepared for partitions yet',
                    "table"),
                hint = format('You should call @extschema@.setup(%L).',
                    "table");
        end if;

        if array_length(@extschema@._overlapping(
                "table", start_value, end_value), 1) > 0 then
            raise 'the partition(s) % overlap the range requested',
                array_to_string(@extschema@._overlapping(
                    "table", start_value, end_value), ', ');
        end if;

        -- Not found: create it
        raise notice '%', format('creating partition %I.%I of table %s',
            @extschema@._schema_name("table"),
            @extschema@._name_for("table", start_value, end_value),
            "table");

        partition = @extschema@._copy_to_subtable("table",
            @extschema@._name_for("table", start_value, end_value));

        -- Insert the data about the partition in the table; the other
        -- functions will get the details from here
        select field_type from @extschema@.partitioned_table t
            where t."table" = create_partition."table"
            into strict type;

        perform @extschema@._register_partition(
            @extschema@._schema_name(partition),
            @extschema@._table_name(partition),
            "table",
            @extschema@._cast(start_value, @extschema@._base_type(type)),
            @extschema@._cast(end_value, @extschema@._base_type(type)));

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


drop function _copy_to_subtable(regclass, text);

create function
_copy_to_subtable("table" regclass, name name) returns regclass
language plpgsql as
$$
declare
    schema name = @extschema@._schema_name("table");
    partition regclass;
begin
    execute format ('create table %I.%I () inherits (%s)',
        schema, name, "table");

    partition = @extschema@._table_oid(schema, name);

    perform @extschema@.copy_constraints("table", partition,
        exclude_types:='{c}');
    perform @extschema@.copy_indexes("table", partition);
    perform @extschema@.copy_triggers("table", partition);
    perform @extschema@.copy_owner("table", partition);
    perform @extschema@.copy_permissions("table", partition);
    perform @extschema@.copy_options("table", partition);

    -- Return the oid of the new table
    return partition;
end
$$;

drop function _month2name(params, int, name);

create function
_month2name(params params, key int)
returns name language sql stable as
$$
    select to_char(@extschema@._month2start(params, key), 'YYYYMM')::name;
$$;

drop function _day2name(params, int, name);

create function
_day2name(params params, key int)
returns name language sql stable as
$$
    select to_char(@extschema@._day2start(params, key), 'YYYYMMDD')::name;
$$;


create or replace function
archive_before("table" regclass, ts timestamptz) returns setof regclass
language plpgsql as
$$
declare
    archive regclass = @extschema@._archive_table("table");
    part regclass;
begin
    if archive is null then
        raise using
            message = format('archive table for %s not found', "table"),
            hint = format(
                'You should run "@extschema@.create_archive(%s)" before.',
                "table");
    end if;

    for part in
        select p.partition from @extschema@.existing_partition p
        where p.partition in (select @extschema@._partitions("table"))
        and p.end_value::timestamptz <= ts
        order by p.start_value::timestamptz
    loop
        raise notice 'archiving partition %', part;
        perform @extschema@._no_inherit(part, "table");
        perform @extschema@._inherit(part, archive);
        return next part;
    end loop;

    perform @extschema@.maintain_insert_function("table");
end
$$;
