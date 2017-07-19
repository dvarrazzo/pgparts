drop function _param_value(params, name);
drop function _param_exists(params, name);
drop function _value2key(regtype, name, params, text);
drop function _value2name(regtype, name, params, text, name);
drop function _values2name(regtype, name, params, text, text, name);
drop function _value2start(regtype, name, params, text);
drop function _value2end(regtype, name, params, text);


create function
_value2name("table" regclass, value text, base_name name) returns name
language plpgsql stable as $$
declare
    value2key text;
    key2name text;
    ft regtype = @extschema@._partition_field_type("table");
    rv text;
begin
    select v.value2key, v.key2name from @extschema@._schema_vtable v
    where v.field_type = ft
    and v.schema_name = @extschema@._partition_schema("table")
    into strict value2key, key2name;

    execute format('select %s($1, %s($1, $2::%s))',
        key2name, value2key, @extschema@._base_type(ft))
    into strict rv using "table", value;
    return base_name || '_' || rv;
end
$$;

create function
_values2name("table" regclass, start_value text, end_value text, base_name name)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2name text;
    ft regtype = @extschema@._partition_field_type("table");
    p1 text;
    p2 text;
begin
    select v.value2key, v.key2name from @extschema@._schema_vtable v
    where v.field_type = ft
    and v.schema_name = @extschema@._partition_schema("table")
    into strict value2key, key2name;

    execute format(
        'select %1$s($1, %2$s($1, $2::%3$s)), %1$s($1, %2$s($1, $3::%3$s))',
        key2name, value2key, @extschema@._base_type(ft))
    into strict p1, p2 using "table", start_value, end_value;

    return base_name || '_' || p1 || '_' || p2;
end
$$;

create function
_value2start("table" regclass, value text) returns name
language plpgsql stable as $$
declare
    value2key text;
    key2start text;
    ft regtype = @extschema@._partition_field_type("table");
    rv text;
begin
    select v.value2key, v.key2start from @extschema@._schema_vtable v
    where v.field_type = ft
    and v.schema_name = @extschema@._partition_schema("table")
    into strict value2key, key2start;

    execute format('select %s($1, %s($1, $2::%s))',
        key2start, value2key, @extschema@._base_type(ft))
    into strict rv using "table", value;
    return rv;
end
$$;

create function
_value2end("table" regclass, value text) returns name
language plpgsql stable as $$
declare
    value2key text;
    key2end text;
    ft regtype = @extschema@._partition_field_type("table");
    rv text;
begin
    select v.value2key, v.key2end from @extschema@._schema_vtable v
    where v.field_type = ft
    and v.schema_name = @extschema@._partition_schema("table")
    into strict value2key, key2end;

    execute format('select %s($1, %s($1, $2::%s))',
        key2end, value2key, @extschema@._base_type(ft))
    into strict rv using "table", value;
    return rv;
end
$$;


create or replace function
_scalar_predicate(partition regclass, prefix text default '') returns text
language sql stable as
$f$
    select format('%L::%I <= %s%I and %s%I < %L::%I',
        p.start_value, typname, prefix, t.field,
        prefix, t.field, p.end_value, typname)
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on t."table" = p.base_table
    join pg_type on t.field_type = pg_type.oid
    where p.partition = $1
$f$;

create or replace function
_range_predicate(partition regclass, prefix text default '') returns text
language sql stable as
$f$
    select format($$%s%I <@ '[%s,%s)'::%I$$,
        prefix, t.field, p.start_value, p.end_value, typname)
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on t."table" = p.base_table
    join pg_type on t.field_type = pg_type.oid
    where p.partition = $1
$f$;
 
create function
_too_old_predicate("table" regclass, prefix text default '') returns text
language sql stable as
$f$
    select format('%s%I < %L::%I',
        prefix, t.field, min(p.start_value), typname)
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on t."table" = p.base_table
    join pg_type on t.field_type = pg_type.oid
    where t."table" = $1
    group by prefix, t.field, typname
$f$;
 
create function
_pkey("table" regclass) returns name
language sql stable as
$$
    select conname from pg_constraint c
    where conrelid = "table"
    and contype = 'p';
$$;

create or replace function
_has_pkey("table" regclass) returns bool
language sql stable as
$$
    select @extschema@._pkey("table") is not null;
$$;


create function
_params("table" regclass) returns params
language sql stable strict as
$$
    select schema_params
    from @extschema@.partitioned_table cfg
    where cfg."table" = $1
$$;

create function
_param("table" regclass, name name, out rv text)
language plpgsql stable strict as
$$
declare
    param text[];
    params @extschema@.params = @extschema@._params("table");
begin
    if params <> '{}' then
        foreach param slice 1 in array params::text[][] loop
            if name = param[1] then
                rv = param[2];
                return;
            end if;
        end loop;
    end if;

    -- return the default
    select "default" into rv
    from @extschema@.schema_param sp
    join @extschema@.partitioned_table pt on pt.schema_name = sp.schema
    where pt."table" = _param."table" and sp.param = _param.name;
end
$$;

create or replace function
name_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2name(name_for."table", value, relname)
    from pg_class r
    where r.oid = name_for."table";
$$;

create or replace function
_name_for("table" regclass, start_value text, end_value text) returns name
language sql stable as
$$
    select @extschema@._values2name("table", start_value, end_value, relname)
    from pg_class r
    where r.oid = _name_for."table";
$$;

create or replace function
start_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2start("table", value);
$$;

create or replace function
end_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2end("table", value);
$$;


create or replace function
_partition_field_type(t regclass) returns regtype
language plpgsql as
$$
declare
    rv regtype;
begin
    select t.field_type
    from @extschema@.partitioned_table t
    where t."table" = $1
    into rv;
    if found then
        return rv;
    end if;

    select t.field_type
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on t."table" = p.base_table
    where p.partition = $1
    into rv;
    if found then
        return rv;
    end if;

    raise object_not_in_prerequisite_state using
        message = format(
            'the table %s is not a partitioned table or a partition', t);
end
$$;
 
create or replace function
_partition_field_nullable(t regclass) returns bool
language plpgsql as
$$
declare
    rv bool;
begin
    select not attnotnull
    from @extschema@.partitioned_table t
    join pg_attribute a on attrelid = t."table" and attname = t.field
    where t."table" = $1
    into rv;
    if found then
        return rv;
    end if;

    select not attnotnull
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on t."table" = p.base_table
    join pg_attribute on attrelid = t."table" and attname = t.field
    where p.partition = $1
    into rv;
    if found then
        return rv;
    end if;

    raise object_not_in_prerequisite_state using
        message = format(
            'the table %s is not a partitioned table or a partition', t);
end
$$;
 
 
create function
_partition_schema(t regclass) returns name
language plpgsql as
$$
declare
    rv name;
begin
    select t.schema_name
    from @extschema@.partitioned_table t
    where t."table" = $1
    into rv;
    if found then
        return rv;
    end if;

    select t.schema_name
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on t."table" = p.base_table
    where p.partition = $1
    into rv;
    if found then
        return rv;
    end if;

    raise object_not_in_prerequisite_state using
        message = format(
            'the table %s is not a partitioned table or a partition', t);
end
$$;

comment on function _partition_schema(regclass) is
$$Return the name of the partitioning schema for a table.

The input table can be either a partitioned table or a partition.
$$;

 

create or replace function
setup("table" regclass, field name, schema_name name,
    schema_params params default '{}')
returns void
language plpgsql as
$$
<<block>>
declare
    field_type regtype;
    param text[];
    param_name name;
    param_type regtype;
    param_default text;
begin
    begin  -- transaction
        -- Check the table is already set up
        perform 1 from @extschema@.partitioned_table t
        where t."table" = setup."table";
        if found then
            raise object_not_in_prerequisite_state using
                message = format(
                    'the table %s is already prepared for partitions',
                    "table"),
                hint = format('Use @extschema@.create_for(%L, VALUE) '
                    'to create new partitions on the table.', "table");
        end if;

        -- Find the type of the field
        select atttypid from pg_attribute a
        where attrelid = "table" and attname = field
        into field_type;
        if not found then
            raise undefined_column using
                message = format(
                    'field %s not found in table %s', field, "table");
        end if;

        -- Does this partitioning schema exist?
        perform 1 from @extschema@._schema_vtable v
            where (v.field_type, v.schema_name)
                = (block.field_type, setup.schema_name);
        if not found then
            raise undefined_parameter using
                message = format('partitioning schema %s on type %s not known',
                    schema_name, field_type);
        end if;

        -- Validate the schema parameters
        if array_ndims(schema_params::text[][]) is not null then
            foreach param slice 1 in array schema_params::text[][] loop
                select type from @extschema@.schema_param sp
                where sp.schema = schema_name and sp.param = block.param[1]
                into param_type;
                if not found then
                    raise undefined_parameter using
                        message = format(
                            'unknown parameter for partitioning schema %s: %s',
                            schema_name, param[1]);
                end if;
                perform @extschema@._valid_for_type(block.param[2], param_type);
            end loop;
        end if;

        perform @extschema@._register_partitioned_table(
            "table", field, field_type, schema_name, schema_params);

        perform @extschema@.maintain_insert_function("table");
        perform @extschema@._create_insert_trigger("table");
        if @extschema@._has_pkey("table") then
            perform @extschema@._create_update_function("table");
            perform @extschema@._create_update_trigger("table");
        end if;

    exception
        -- you can't have this clause empty
        when division_by_zero then raise 'wat?';
    end;
end
$$;
 

create or replace function
_maintain_insert_function_empty("table" regclass) returns void
language plpgsql as $body$
declare
    schema name = @extschema@._schema_name("table");
    fname name = @extschema@._table_name("table") || '_partition_insert';
    field name;
    null_check text;
begin
    select t.field from @extschema@.partitioned_table t
    where t."table" = _maintain_insert_function_empty."table"
    into strict field;

    null_check := @extschema@._null_insert_snippet("table");

    execute format(
$f$
        create or replace function %I.%I()
        returns trigger language plpgsql as $$
begin
%s
    raise undefined_table using
        message = 'no partition available on table %s',
        hint = format(
            $m$You should call @extschema@.create_for(%L, %%L).$m$, new.%I);
end
$$
$f$,
        schema, fname, null_check, "table", "table", field);
end
$body$;

 
create function
_drop_old_snippet("table" regclass) returns text
language plpgsql stable as
$f$
begin
    if not @extschema@._param("table", 'drop_old')::bool then
        return '';
    else
        return format(
$$
    if %s then
        -- discard data too old
        return null;
    end if;
$$,
        @extschema@._too_old_predicate("table", 'new.'));
    end if;

end
$f$;


create function
_on_conflict_snippet("table" regclass, partition regclass) returns text
language plpgsql stable as
$f$
begin
    if not @extschema@._param("table", 'on_conflict_drop')::bool then
        return '';
    else
        return format($$
            on conflict on constraint %I do nothing$$,
            @extschema@._pkey(partition));
    end if;
end
$f$;
 

create or replace function
_scalar_insert_snippet(partition regclass) returns text
language sql stable as
$f$
    select format(
$$
    if %s then
        insert into %I.%I values (new.*)%s;
        return null;
    end if;
$$,
        @extschema@._scalar_predicate(p.partition, 'new.'),
        p.schema_name, p.table_name,
        @extschema@._on_conflict_snippet(p.base_table, p.partition))
    from @extschema@.existing_partition p
    where p.partition = $1
$f$;


create or replace function
_maintain_insert_function_scalar("table" regclass) returns void
language plpgsql as $body$
declare
    schema name = @extschema@._schema_name("table");
    fname name = @extschema@._table_name("table") || '_partition_insert';
    field name;
    null_check text;
    old_check text;
    checks text;
begin
    select t.field from @extschema@.partitioned_table t
    where t."table" = $1
    into strict field;

    select array_to_string(array_agg(s), '')
    from (
        select @extschema@._scalar_insert_snippet(p.partition) s
        from @extschema@.existing_partition p
        where p.partition in (select @extschema@._partitions("table"))
        order by p.partition::text desc) x
    into strict checks;

    null_check := @extschema@._null_insert_snippet("table");
    old_check := @extschema@._drop_old_snippet("table");

    execute format(
$f$
create or replace function %I.%I()
returns trigger language plpgsql as $$
begin
%s%s%s
    raise undefined_table using
        message = format(
            $m$partition %I.%%I missing for %I = %%L$m$,
            @extschema@.name_for(%L::regclass, new.%I::text), new.%I),
        hint = format(
            $m$You should call @extschema@.create_for(%L, %%L).$m$, new.%I);
end
$$
$f$,
        schema, fname, null_check, checks, old_check,
        schema, field, "table", field, field,
        "table", field);
end
$body$;


create or replace function
_range_insert_snippet(partition regclass) returns text
language sql stable as
$f$
    select format(
$$
    if %L::%I && new.%I then
        if %L::%I @> new.%I then
            insert into %I.%I values (new.*);
            return null;
        else
            rest = new.%I - %L::%I;
            new.%I = new.%I * %L::%I;
            insert into %I.%I values (new.*);
            new.%I = rest;
        end if;
    end if;
$$,
        range, typname, field,              -- if
        range, typname, field,              -- if
        schema_name, table_name,            -- insert into
        field, range, typname,              -- rest =
        field, field, range, typname,       -- new.x =
        schema_name, table_name, field)     -- insert into
    from (select
        t.field, p.schema_name, p.table_name, typname,
            format('[%s,%s)', p.start_value, p.end_value) as range
        from @extschema@.existing_partition p
        join @extschema@.partitioned_table t on t."table" = p.base_table
        join pg_type on t.field_type = pg_type.oid
        where p.partition = $1) x
$f$;
 

create or replace function
_maintain_insert_function_range("table" regclass) returns void
language plpgsql as $body$
declare
    schema name = @extschema@._schema_name("table");
    fname name = @extschema@._table_name("table") || '_partition_insert';
    field name;
    type regtype;
    null_check text;
    checks text;
begin
    select t.field, t.field_type from @extschema@.partitioned_table t
    where t."table" = $1
    into strict field, type;

    select array_to_string(array_agg(s), '')
    from (
        select @extschema@._range_insert_snippet(p.partition) s
        from @extschema@.existing_partition p
        where p.partition in (select @extschema@._partitions("table"))
        order by p.partition::text desc) x
    into strict checks;

    null_check := @extschema@._null_insert_snippet("table");

    execute format(
$f$
create or replace function %I.%I()
returns trigger language plpgsql as $$
declare
    rest %I;
begin
%s%s
    if new.%I = 'empty' then
        raise invalid_parameter_value using
            message = $m$the field %I cannot be 'empty'$m$;
    end if;

    raise undefined_table using
        message = format(
            $m$partition %I.%%I missing for %I = %%L$m$,
            @extschema@.name_for(%L::regclass, lower(new.%I)::text), new.%I),
        hint = format(
            $m$You should call @extschema@.create_for(%L, %%L).$m$,
            lower(new.%I));
end
$$
$f$,
        schema, fname, type, null_check, checks,
        field, field,
        schema, field, "table", field, field,
        "table", field);
end
$body$;


create or replace function
_create_update_function("table" regclass) returns void
language plpgsql as $body$
declare
    sname name = @extschema@._schema_name("table");
    tname name = @extschema@._table_name("table");
    fname name = tname || '_partition_update';
    pkey text;
begin

    -- the snippet to match the record to delete
    select array_to_string(
        array_agg(format('%1$I = old.%1$I', attname)), ' and ')
    from pg_constraint c
    join pg_attribute a on attrelid = conrelid and attnum = any (conkey)
    where conrelid = "table" and contype = 'p'
    into strict pkey;

    execute format($f$
        create or replace function %I.%I() returns trigger
            language plpgsql as $$
begin
    delete from %I.%I where %s;
    insert into %I.%I values (new.*);
    return null;
end
$$
        $f$, sname, fname,
        sname, tname, pkey,
        sname, tname);
end
$body$;

create or replace function
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
            raise object_not_in_prerequisite_state using
                message = format(
                    'the table %s has not been prepared for partitions yet',
                    "table"),
                hint = format('You should call @extschema@.setup(%L).',
                    "table");
        end if;

        if array_length(@extschema@._overlapping(
                "table", start_value, end_value), 1) > 0 then
            raise invalid_parameter_value using
                message = format(
                    'the partition(s) %s overlap the range requested',
                    array_to_string(@extschema@._overlapping(
                        "table", start_value, end_value), ', '));
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
            raise object_not_in_prerequisite_state using
                message = format(
                    'the table %s has not been prepared for partitions yet',
                    "table"),
                hint = format('You should call @extschema@.setup(%L).',
                    "table");
        elsif info.state = 'present' then
            return info.partition;
        elsif info.state = 'detached' then
            raise object_not_in_prerequisite_state using
                message = format('the partition %s exists but is detached',
                    info.partition),
                hint = format('You can attach it back using '
                        '@extschema@.attach_for(%L, %L).',
                    "table", value);
        elsif info.state = 'archived' then
            raise object_not_in_prerequisite_state using
                message = format('the partition %s exists but was archived',
                    info.partition),
                hint = format('You can re-enable it using '
                        '@extschema@.unarchive(%L).',
                    info.partition);
        elsif info.state = 'missing' then
            null;
        else
            raise internal_error using
                message = format(
                    'unexpected partition state: %s', info.state);
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
            raise invalid_parameter_value using
                message = format(
                    'the partition(s) %s overlap the range requested',
                    array_to_string(@extschema@._overlapping(
                        "table", start_value, end_value), ', '));
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


create or replace function
_register_partition(
    schema_name name, table_name name, base_table regclass,
    start_value text, end_value text) returns void
language plpgsql security definer as
$$
begin
    -- Delete an eventual record of a partition created and dropped
    delete from @extschema@.partition p
        where (p.schema_name, p.table_name)
            = (_register_partition.schema_name, _register_partition.table_name)
        and not exists (
            select 1 from pg_class
            where oid = p.base_table);

    insert into @extschema@.partition
        (schema_name, table_name, base_table, start_value, end_value)
    values
        (schema_name, table_name, base_table, start_value, end_value);
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
        raise undefined_table using
            message = format('there is no %I partition for %L',
                "table", value);
    end if;

    if partition in (select @extschema@._partitions("table")) then
        raise notice '%', format('detaching partition %s from %s',
            partition, "table");
        perform @extschema@._no_inherit(partition, "table");
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
        raise undefined_table using
            message = format('there is no %I partition for %L',
                "table", value);
    end if;

    if partition not in (select @extschema@._partitions("table")) then
        raise notice '%', format('attaching partition %s to %s',
            partition, "table");
        perform @extschema@._inherit(partition, "table");
        perform @extschema@.maintain_insert_function("table");
    end if;

    return partition;
end
$$;


create or replace function
create_archive("table" regclass) returns regclass
language plpgsql as
$$
/* Create an archival area for rotated partitions
   e.g. archival area on table foo will be foo_all, which also contains
   foo_archived. Once a partition foo_201705 is archived it can go into
   _archived, so that querying the foo table will not recurse there but the
   data is still accessible from foo_all.

    foo_all
     +- foo                     <- live partitions
     |   +- foo_201705
     |   +- foo_201706
     |   +- foo_201707
     +- foo_archived            <- archived partitions
         +- foo_201703
         +- foo_201704
*/

declare
    tname name = @extschema@._table_name("table");
    sname name = @extschema@._schema_name("table");
    rv regclass = @extschema@._archive_table("table");
begin
    if rv is not null then
        return rv;
    end if;

    raise notice 'creating table %_all', tname;
    execute format('create table %I.%I (like %I.%I)',
        sname, tname || '_all', sname, tname);
    execute format('alter table %I.%I inherit %I.%I',
        sname, tname, sname, tname || '_all');

    raise notice 'creating table %_archived', tname;
    execute format('create table %I.%I () inherits (%I.%I)',
        sname, tname || '_archived', sname, tname || '_all');

    rv = @extschema@._archive_table("table");
    if rv is null then
        raise internal_error using
            message = 'uhm, I should have created this table...';
    end if;

    return rv;
end
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
        raise undefined_table using
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


create or replace function
unarchive_partition(part regclass) returns regclass
language plpgsql as
$$
declare
    state partition_state = @extschema@._partition_state(part);
    archive regclass;
    parent regclass;
begin
    if state != 'archived' then
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


insert into schema_param values (
    'monthly', 'drop_old', 'bool', 'false',
    'Discard records going to partitions not available in the past.');

insert into schema_param values (
    'monthly', 'on_conflict_drop', 'bool', 'false',
$$If an insert in a partition has a primary key conflict drop the record.

The strategy can be used with tables receiving replication updates from a
synchronized table, to cope with an occasional loss of sync: the replication
can be rewinded a bit and further updates can revert the table into a
consistent state.$$);


drop function _month2key(params, timestamptz);
drop function _month2start(params, int);
drop function _month2end(params, int);
drop function _month2name(params, int);
drop function _month2keytz(params, timestamptz);
drop function _month2starttz(params, int);
drop function _month2endtz(params, int);

create function
_month2key("table" regclass, value timestamptz) returns int
language sql stable as
$$
    select
        ((12 * (date_part('year', $2) - 1970)
            + date_part('month', $2) - 1)::int
        / @extschema@._param("table", 'nmonths')::int)
        * @extschema@._param("table", 'nmonths')::int;
$$;

create function
_month2start("table" regclass, key int) returns date
language sql stable as
$$
    select ('epoch'::date + '1 month'::interval * key)::date;
$$;

create function
_month2end("table" regclass, key int) returns date
language sql stable as $$
    select (@extschema@._month2start("table", key)
        + '1 month'::interval
        * @extschema@._param("table", 'nmonths')::int)::date;
$$;

create function
_month2name("table" regclass, key int)
returns name language sql stable as
$$
    select to_char(@extschema@._month2start("table", key), 'YYYYMM')::name;
$$;
 
create function
_month2keytz("table" regclass, value timestamptz) returns int
language sql stable as
$$
    select @extschema@._month2key("table",
        value at time zone @extschema@._param("table", 'timezone'));
$$;

create function
_month2starttz("table" regclass, key int) returns timestamptz
language sql stable as
$$
    select @extschema@._month2start("table", key)::timestamp
        at time zone @extschema@._param("table", 'timezone');
$$;

create function
_month2endtz("table" regclass, key int) returns timestamptz
language sql stable as $$
    select @extschema@._month2end("table", key)::timestamp
        at time zone @extschema@._param("table", 'timezone');
$$;


insert into schema_param values (
    'daily', 'drop_old', 'bool', 'false',
    'Discard records going to partitions not available in the past.');

insert into schema_param values (
    'daily', 'on_conflict_drop', 'bool', 'false',
$$If an insert in a partition has a primary key conflict drop the record.

The strategy can be used with tables receiving replication updates from a
synchronized table, to cope with an occasional loss of sync: the replication
can be rewinded a bit and further updates can revert the table into a
consistent state.$$);


drop function _day2key(params, timestamptz);
drop function _day2start(params, int);
drop function _day2end(params, int);
drop function _day2name(params, int);
drop function _day2keytz(params, timestamptz);
drop function _day2starttz(params, int);
drop function _day2endtz(params, int);

create function
_day2key("table" regclass, value timestamptz) returns int
language sql stable as
$$
    -- The 3 makes weekly partitions starting on Sunday with offset 0
    -- which is consistent with extract(dow), if anything.
    select
        (((value::date - 'epoch'::date)::int
            - @extschema@._param("table", 'start_dow')::int - 3)
        / @extschema@._param("table", 'ndays')::int)
        * @extschema@._param("table", 'ndays')::int;
$$;

create function
_day2start("table" regclass, key int) returns date
language sql stable as
$$
    select ('epoch'::date + '1 day'::interval * (key + 3
        + @extschema@._param("table", 'start_dow')::int))::date;
$$;

create function
_day2end("table" regclass, key int) returns date
language sql stable as $$
    select (@extschema@._day2start("table", key)
        + '1 day'::interval
        * @extschema@._param("table", 'ndays')::int)::date;
$$;

create function
_day2name("table" regclass, key int)
returns name language sql stable as
$$
    select to_char(@extschema@._day2start("table", key), 'YYYYMMDD')::name;
$$;

create function
_day2keytz("table" regclass, value timestamptz) returns int
language sql stable as
$$
    select @extschema@._day2key("table",
        value at time zone @extschema@._param("table", 'timezone'));
$$;

create function
_day2starttz("table" regclass, key int) returns timestamptz
language sql stable as
$$
    select @extschema@._day2start("table", key)::timestamp
        at time zone @extschema@._param("table", 'timezone');
$$;

create function
_day2endtz("table" regclass, key int) returns timestamptz
language sql stable as $$
    select @extschema@._day2end("table", key)::timestamp
        at time zone @extschema@._param("table", 'timezone');
$$;
 
create or replace function
rename_partition(src "regclass", dst name) returns void
language plpgsql as $$
-- Rename a table in a consistent way
declare
    tbl regclass;
begin
    begin -- transaction
        -- Rename the partition in the registry
        update @extschema@.partition
        set table_name = dst
        where schema_name = @extschema@._schema_name(src)
        and table_name = @extschema@._table_name(src)
        returning base_table into tbl;

        if not found then
            raise object_not_in_prerequisite_state using
                message = format('the table %I doesn''t seem a partition', src);
        end if;

        -- Rename the table
        execute format('alter table %s rename to %I', src, dst);

        -- Keep the dispatch function consistent
        perform @extschema@.maintain_insert_function(tbl);

    exception
        -- you can't have this clause empty
        when division_by_zero then raise 'wat?';
    end;
end
$$;
