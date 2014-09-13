--
-- pgparts -- simple tables partitioning for PostgreSQL
--
-- Copyright (C) 2014 Daniele Varrazzo <daniele.varrazzo@gmail.com>
--

-- Tables used to memorize partitioned tables {{{

create table partition_schema (
    field_type regtype,
    name name,
    primary key (field_type, name),

    params text[] not null,
    description text
);

comment on table partition_schema is
    'The partitioning schemas the system knows';


create table partitioned_table (
    "table" regclass primary key,
    field name not null,

    field_type regtype not null,
    schema_name text not null,
    foreign key (field_type, schema_name)
        references partition_schema (field_type, name),

    schema_params text[] not null
);

comment on table partitioned_table is
    'The partitioning parameters for the tables prepared to create partitions';


create table partition (
    partition regclass primary key,
    base_table regclass not null references partitioned_table ("table"),
    start_value text not null,
    end_value text not null);

comment on table partition is
    'The ranges covered by the single partitions';


-- }}}

-- Virtual methods dispatch {{{

create table _schema_vtable (
    field_type regtype,
    name name,
    primary key (field_type, name),
    foreign key (field_type, name)
        references partition_schema (field_type, name)
        on update cascade on delete cascade,

    value2key name not null,
    key2name name not null,
    key2start name not null,
    key2end name not null
);


create function _value2key(
    field_type regtype, schema_name name, params text[], value text)
returns text language plpgsql stable as $$
declare
    value2key text;
    rv text;
begin
    select v.value2key from @extschema@._schema_vtable v
    where (v.field_type, v.name)
        = (_value2key.field_type, _value2key.schema_name)
    into strict value2key;

    execute 'select ' || value2key || '($1, $2::' || field_type || ')'
    into strict rv using params, value;
    return rv;
end
$$;

create function _value2name(
    field_type regtype, schema_name name, params text[],
    value text, base_name name)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2name text;
    rv text;
begin
    select v.value2key, v.key2name from @extschema@._schema_vtable v
    where (v.field_type, v.name)
        = (_value2name.field_type, _value2name.schema_name)
    into strict value2key, key2name;

    execute 'select ' || key2name
        || '($1, ' || value2key || '($1, $2::' || field_type || ')::text, $3)'
    into strict rv using params, value, base_name;
    return rv;
end
$$;

create function _value2start(
    field_type regtype, schema_name name, params text[], value text)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2start text;
    rv text;
begin
    select v.value2key, v.key2start from @extschema@._schema_vtable v
    where (v.field_type, v.name)
        = (_value2start.field_type, _value2start.schema_name)
    into strict value2key, key2start;

    execute 'select ' || key2start
        || '($1, ' || value2key || '($1, $2::' || field_type || ')::text)'
    into strict rv using params, value;
    return rv;
end
$$;

create function _value2end(
    field_type regtype, schema_name name, params text[], value text)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2end text;
    rv text;
begin
    select v.value2key, v.key2end from @extschema@._schema_vtable v
    where (v.field_type, v.name)
        = (_value2end.field_type, _value2end.schema_name)
    into strict value2key, key2end;

    execute 'select ' || key2end
        || '($1, ' || value2key || '($1, $2::' || field_type || ')::text)'
    into strict rv using params, value;
    return rv;
end
$$;


-- }}}

-- Informative functions {{{

create function _table_name("table" regclass) returns name
language sql stable as
$$
    select relname from pg_class where oid = $1;
$$;

create function _schema_name("table" regclass) returns name
language sql stable as
$$
    select nspname
    from pg_class c
    join pg_namespace n on n.oid = relnamespace
    where c.oid = $1;
$$;

create function name_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2name(
        cfg.field_type, cfg.schema_name, cfg.schema_params,
        value, relname)
    from pg_class r
    join @extschema@.partitioned_table cfg on r.oid = cfg."table"
    where cfg."table" = name_for."table";
$$;

create function start_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2start(
        cfg.field_type, cfg.schema_name, cfg.schema_params, value)
    from pg_class r
    join @extschema@.partitioned_table cfg on r.oid = cfg."table"
    where cfg."table" = start_for."table";
$$;

create function end_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2end(
        cfg.field_type, cfg.schema_name, cfg.schema_params, value)
    from pg_class r
    join @extschema@.partitioned_table cfg on r.oid = cfg."table"
    where cfg."table" = end_for."table";
$$;

create function partition_for("table" regclass, value text) returns regclass
language sql as
$$
    select c.oid
    from pg_class c
    join pg_namespace n on c.relnamespace = n.oid
    where relname = @extschema@.name_for("table", value)
    and nspname = @extschema@._schema_name("table");
$$;


create type partition_state as
    enum ('unpartitioned', 'missing', 'present', 'detached');

create type partition_info as (
    state @extschema@.partition_state,
    partition regclass);

create function info("table" regclass, value text)
returns @extschema@.partition_info
language plpgsql stable as
$$
declare
    rv @extschema@.partition_info;
begin
    perform 1 from @extschema@.partitioned_table pt
    where pt."table" = info.table;
    if not found then
        rv.state = 'unpartitioned';
        return rv;
    end if;

    rv.partition = @extschema@.partition_for("table", value);
    if rv.partition is null then
        rv.state = 'missing';
        return rv;
    end if;

    perform 1 from pg_inherits
    where inhparent = "table" and inhrelid = rv.partition;
    if found then
        rv.state = 'present';
    else
        rv.state = 'detached';
    end if;

    return rv;
end
$$;

create function _partitions("table" regclass) returns setof regclass
language sql as $$
    select p.partition
    from @extschema@.partition p
    join pg_inherits i on p.partition = inhrelid
    where p.base_table = "table" and inhparent = "table";
$$;


-- }}}

-- Setting up a partitioned table {{{

create function setup(
    "table" regclass, field name, schema_name name, schema_params text[])
returns void
language plpgsql as
$$
declare
    field_type regtype;
begin
    begin  -- transaction
        -- Check the table is already set up
        perform 1 from @extschema@.partitioned_table t
        where t."table" = setup."table";
        if found then
            raise using
                message = format('the table %s is already partitioned',
                    "table"),
                hint = format('Use @extschema@.create_for(%L, VALUE) '
                    'to create new partitions on the table.', "table");
        end if;

        -- Find the type of the field
        select atttypid from pg_attribute a
        where attrelid = "table" and attname = field
        into field_type;
        if not found then
            raise 'field % not found in table %', field, "table";
        end if;

        insert into @extschema@.partitioned_table
            ("table", field, field_type, schema_name, schema_params)
        values ("table", field, field_type, schema_name, schema_params);

        perform @extschema@._maintain_insert_function("table");
        perform @extschema@._create_insert_trigger("table");
        perform @extschema@._create_update_function("table");

    exception
        -- you can't have this clause empty
        when division_by_zero then raise 'wat?';
    end;
end
$$;

create function _maintain_insert_function("table" regclass) returns void
language plpgsql as $$
declare
    nparts int;
begin
    select count(*) from @extschema@._partitions("table")
    into nparts;

    if nparts = 0 then
        perform @extschema@._maintain_insert_function_empty("table");
    else
        perform @extschema@._maintain_insert_function_parts("table");
    end if;
end
$$;

create function _maintain_insert_function_empty("table" regclass) returns void
language plpgsql as $body$
declare
    schema name = @extschema@._schema_name("table");
    fname name = @extschema@._table_name("table") || '_partition_insert';
    field name;
begin
    select t.field from @extschema@.partitioned_table t
    where t."table" = _maintain_insert_function_empty."table"
    into strict field;

    execute format(
$f$
        create or replace function %I.%I()
        returns trigger language plpgsql as $$
begin
    raise using
        message = 'no partition available on table %s',
        hint = format(
            $m$You should call @extschema@.create_for(%L, %%L).$m$, new.%I);
end
$$
$f$,
        schema, fname, "table", "table", field);
end
$body$;

create function _maintain_insert_function_parts("table" regclass) returns void
language plpgsql as $body$
declare
    schema name = @extschema@._schema_name("table");
    fname name = @extschema@._table_name("table") || '_partition_insert';
    field name;
    checks text;
begin
    select t.field from @extschema@.partitioned_table t
    where t."table" = _maintain_insert_function_parts."table"
    into strict field;

    with checks(body) as (
        select format(
$c$
    if %L <= new.%I and new.%I < %L then
        insert into %s values (new.*);
        return null;
    end if;
$c$,
            p.start_value, field, field, p.end_value, p.partition)
        from @extschema@.partition p
        where p.partition in (select @extschema@._partitions("table"))
        order by p.partition::text desc)
    select array_to_string(array_agg(body), '') from checks
    into strict checks;

    execute format(
$f$
create or replace function %I.%I()
returns trigger language plpgsql as $$
begin
%s
    raise using
        message = format(
            $m$partition on table %s missing for %I = %%L$m$, new.%I),
        hint = format(
            $m$You should call @extschema@.create_for(%L, %%L).$m$, new.%I);
end
$$
$f$,
        schema, fname, checks,
        "table", field, field, "table", field);

end
$body$;

create function _create_insert_trigger("table" regclass) returns void
language plpgsql as $body$
declare
    sname name = @extschema@._schema_name("table");
    fname name = @extschema@._table_name("table") || '_partition_insert';
    -- It should be the last of the triggers "before"
    -- But don't use a 'zzz' prefix as it clashes with pg_repack
    tname name = 'yyy_partition_insert';
begin
    execute format($t$
        create trigger %I before insert on %s
        for each row execute procedure %I.%I();
        $t$, tname, "table", sname, fname);
end
$body$;

-- The function created is used by triggers on the partitions,
-- not on the base table
create function _create_update_function("table" regclass) returns void
language plpgsql as $body$
declare
    schema name = @extschema@._schema_name("table");
    fname name = @extschema@._table_name("table") || '_partition_update';
    pkey text;
begin

    -- the snippet to match the record to delete
    select array_to_string(
        array_agg(format('%1$I = old.%1$I', attname)), ' and ')
    from pg_constraint c
    join pg_attribute a on attrelid = conrelid and attnum = any (conkey)
    where conrelid = "table" and contype = 'p'
    into pkey;
    if pkey is null then
        raise 'the table % doesn''t have a primary key', "table";
    end if;

    execute format($f$
        create function %I.%I() returns trigger language plpgsql as $$
begin
    delete from %s where %s;
    insert into %s values (new.*);
    return null;
end
$$
        $f$, schema, fname, "table", pkey, "table");
end
$body$;


-- }}}

-- Setting up a partition {{{

create function create_for("table" regclass, value text) returns regclass
language plpgsql as
$$
declare
    pname name;
    info @extschema@.partition_info;
    partition regclass;
begin
    begin  -- transaction
        select (@extschema@.info("table", value)).* into strict info;
        if info.state = 'unpartitioned' then
            raise using
                message = format('the table %s has not been partitioned yet',
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
        select @extschema@._copy_to_subtable("table", value)
        into strict partition;

        -- Insert the data about the partition in the table; the other
        -- functions will get the details from here
        insert into @extschema@.partition
            (partition, base_table, start_value, end_value)
        values (
            partition, "table",
            @extschema@.start_for("table", value),
            @extschema@.end_for("table", value));

        perform @extschema@._create_partition_update_trigger(partition);
        perform @extschema@._constraint_partition(partition);
        perform @extschema@._maintain_insert_function("table");

    exception
        -- you can't have this clause empty
        when division_by_zero then raise 'wat?';
    end;

    return partition;
end
$$;

create function detach_for("table" regclass, value text) returns regclass
language plpgsql as
$body$
declare
    partition regclass = @extschema@.partition_for("table", value);
begin
    if partition is null then
        raise using
            message = format('there is no %I partition for %L',
                "table", value);
    end if;

    if partition in (select @extschema@._partitions("table")) then
        execute format('alter table %s no inherit %s',
            partition, "table");
        perform @extschema@._maintain_insert_function("table");
    end if;

    return partition;
end
$body$;

create function attach_for("table" regclass, value text) returns regclass
language plpgsql as
$body$
declare
    partition regclass = @extschema@.partition_for("table", value);
begin
    if partition is null then
        raise using
            message = format('there is no %I partition for %L',
                "table", value);
    end if;

    if partition not in (select @extschema@._partitions("table")) then
        execute format('alter table %s inherit %s',
            partition, "table");
        perform @extschema@._maintain_insert_function("table");
    end if;

    return partition;
end
$body$;


create function _copy_to_subtable("table" regclass, value text) returns regclass
language plpgsql as
$$
declare
    name name = @extschema@.name_for("table", value);
begin
    execute format ('create table %I.%I () inherits (%s)',
        @extschema@._schema_name("table"), name, "table");
    -- TODO: inherit the rest

    -- Return the oid of the new table
    return @extschema@.partition_for("table", value);
end
$$;

create function _create_partition_update_trigger(partition regclass) returns void
language plpgsql as $f$
declare
    base_table regclass;
    field name;
    start_value text;
    end_value text;
    fname name;
    sname name;
    -- It should be the last of the triggers "before"
    -- But don't use a 'zzz' prefix as it clashes with pg_repack
    tname name = 'yyy_partition_update';
begin
    select t.field, p.start_value, p.end_value,
        -- Defined by _create_update_function() in setup()
        @extschema@._table_name(p.base_table) || '_partition_update',
        @extschema@._schema_name(p.base_table)
    from @extschema@.partition p
    join @extschema@.partitioned_table t on p.base_table = t."table"
    into field, start_value, end_value, fname, sname;

    execute format($t$
        create trigger %I before update on %s
        for each row when (not (%L <= new.%I and new.%I < %L))
        execute procedure %I.%I();
        $t$, tname, partition, start_value, field, field, end_value, sname, fname);
end
$f$;

create function _constraint_partition(partition regclass) returns void
language plpgsql as $f$
declare
    partname name := @extschema@._table_name(partition);
    field name;
    start_value text;
    end_value text;
begin
    select t.field, p.start_value, p.end_value
    from @extschema@.partition p
    join @extschema@.partitioned_table t on p.base_table = t."table"
    where p.partition = _constraint_partition.partition
    into strict field, start_value, end_value;

    execute format(
        'alter table %s add constraint %I check (%L <= %I and %I < %L)',
        partition, partname || '_partition_chk',
        start_value, field, field, end_value);
end
$f$;


-- }}}

-- Partitioning schemas implementations {{{

create function _month2key(params text[], value date) returns int
language sql stable as
$$
    select ((12 * date_part('year', $2) + date_part('month', $2) - 1)::int
        / params[1]::int) * params[1]::int;
$$;

create function _month2start(params text[], key text) returns date
language sql stable as
$$
    select ('0001-01-01'::date
        + '1 month'::interval * key::int
        - '1 year'::interval)::date;
$$;

create function _month2end(params text[], key text) returns date
language sql stable as $$
    select (@extschema@._month2start(params, key)
        + '1 month'::interval * params[1]::int)::date;
$$;

create function _month2name(params text[], key text, base_name name)
returns name language sql stable as
$$
    select (base_name || '_'
        || to_char(@extschema@._month2start(params, key), 'YYYYMM'))::name;
$$;

insert into partition_schema values (
    'date'::regtype, 'monthly', '{months_per_partiton}',
$$Each partition of the table contains 'months_per_partiton' months.

The partitioning triggers checks the partitions from the newest to the oldest
so, if normal inserts happens in order of time, dispatching to the right
partition should be o(1), whereas for random inserts dispatching is o(n) in the
number of partitions.
$$);

insert into _schema_vtable values (
    'date'::regtype, 'monthly',
    '@extschema@._month2key', '@extschema@._month2name',
    '@extschema@._month2start', '@extschema@._month2end');


-- }}}
