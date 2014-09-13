-- Tables used to memorize partitioned tables {{{

create table partition_schema (
    field_type regtype,
    name name,
    primary key(field_type, name),

    params text[] not null,

    -- vtable
    value2key name not null,
    key2name name not null,
    key2start name not null,
    key2end name not null
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

-- Informative functions {{{

create type partition_state as
    enum ('unpartitioned', 'missing', 'present', 'detached');

create type partition_info as (
    state @extschema@.partition_state,
    table_schema name,
    "table" regclass,
    field name,
    field_type regtype,
    schema_name name,
    schema_params text[],
    partition regclass,
    name name);

create function info("table" regclass, name name)
returns @extschema@.partition_info
language plpgsql stable as
$$
declare
    rv @extschema@.partition_info;
begin
    select @extschema@.schema_name(t.oid), t.oid,
        pt.field, pt.field_type, pt.schema_name, pt.schema_params,
        case when pt."table" is null then 'unpartitioned' end
    from pg_class t
    left join @extschema@.partitioned_table pt on pt."table" = t.oid
    where t.oid = info."table"
    into rv.table_schema, rv."table",
        rv.field, rv.field_type, rv.schema_name, rv.schema_params,
        rv.state;
    if rv.state = 'unpartitioned' then
        return rv;
    end if;

    rv.name := name;

    select p.oid,
        (case when inhparent is not null
            then 'present' else 'detached' end)::@extschema@.partition_state
    from pg_class p
    join pg_namespace n on n.oid = p.relnamespace
    left join pg_inherits i
        on inhparent = "table" and inhrelid = p.oid
    where p.relname = name
    and nspname = rv.table_schema
    into rv.partition, rv.state;

    if rv.state is null then
        rv.state := 'missing';
    end if;

    return rv;
end
$$;

create function table_name("table" regclass) returns name
language sql stable as
$$
    select relname from pg_class where oid = $1;
$$;

create function schema_name("table" regclass) returns name
language sql stable as
$$
    select nspname
    from pg_class c
    join pg_namespace n on n.oid = relnamespace
    where c.oid = $1;
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
        -- find the type of the field
        select atttypid from pg_attribute a
        where attrelid = "table" and attname = field
        into field_type;
        if not found then
            raise 'field % not found in table %', field, "table";
        end if;

        insert into @extschema@.partitioned_table
            ("table", field, field_type, schema_name, schema_params)
        values ("table", field, field_type, schema_name, schema_params);

        perform @extschema@.maintain_insert_function("table");
        perform @extschema@.create_insert_trigger("table");
        perform @extschema@.create_update_function("table");

    exception
        -- you can't have this clause empty
        when division_by_zero then raise 'wat?';
    end;
end
$$;

create function maintain_insert_function("table" regclass) returns void
language plpgsql as $body$
declare
    schema name = @extschema@.schema_name("table");
    fname name = @extschema@.table_name("table") || '_partition_insert';
    field name;
    nparts int;
    checks text;
begin
    select t.field, count(p.partition)
    from @extschema@.partitioned_table t
    left join @extschema@.partition p on t."table" = p.base_table
    where t."table" = maintain_insert_function."table"
    group by 1
    into strict field, nparts;

    if nparts = 0 then  -----------------------------------

        execute format(
$f$
            create or replace function %I.%I()
            returns trigger language plpgsql as $$
begin
    raise using
        message = 'no partition available on table %I',
        hint = format(
            $m$You should call @extschema@.create_for(%L, %%L)$m$, new.%I);
end
$$
$f$,
            schema, fname, "table", "table", field);

    else    -----------------------------------------------

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
            where p.base_table = "table"
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
            $m$partition on table %I missing for %I = %%L$m$, new.%I),
        hint = format(
            $m$You should call @extschema@.create_for(%L, %%L)$m$, new.%I);
end
$$
$f$,
            schema, fname, checks,
            "table", field, field, "table", field);

    end if; -----------------------------------------------
end
$body$;

create function create_insert_trigger("table" regclass) returns void
language plpgsql as $body$
declare
    fname name = @extschema@.table_name("table") || '_partition_insert';
    -- It should be the last of the triggers "before"
    -- But don't use a 'zzz' prefix as it clashes with pg_repack
    tname name = 'yyy_partition_insert';
begin
    execute format($t$
        create trigger %I before insert on %s
        for each row execute procedure %s();
        $t$, tname, "table", fname);
end
$body$;

-- The function created is used by triggers on the partitions,
-- not on the base table
create function create_update_function("table" regclass) returns void
language plpgsql as $body$
declare
    schema name = @extschema@.schema_name("table");
    fname name = @extschema@.table_name("table") || '_partition_update';
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
        select @extschema@.name_for("table", value) into strict pname;

        -- Check if exists
        select (@extschema@.info("table", pname)).* into strict info;
        if info.state = 'unpartitioned' then
            raise using
                message = format('the table %s has not been partitioned yet',
                    "table"),
                hint = format('You should call @extschema@.setup(%L).',
                    "table");
        elsif info.state = 'present' then
            return info.partition;
        elsif info.state = 'detached' then
            raise 'the partition % exists but is detached', pname;
        elsif info.state = 'missing' then
            null;
        else
            raise 'unexpected partition state: %', info.state;
        end if;

        -- Not found: create it
        select @extschema@.copy_to_subtable("table", value)
        into strict partition;

        -- Insert the data about the partition in the table; the other
        -- functions will get the details from here
        insert into @extschema@.partition
            (partition, base_table, start_value, end_value)
        values (
            partition, "table",
            @extschema@.start_for("table", value),
            @extschema@.end_for("table", value));

        perform @extschema@.create_partition_update_trigger(partition);
        perform @extschema@.constraint_partition(partition);
        perform @extschema@.maintain_insert_function("table");

    exception
        -- you can't have this clause empty
        when division_by_zero then raise 'wat?';
    end;

    return partition;
end
$$;

create function copy_to_subtable("table" regclass, value text) returns regclass
language plpgsql as
$$
declare
    rv regclass;
    name name = @extschema@.name_for("table", value);
begin
    execute format ('create table %I.%I () inherits (%s)',
        @extschema@.schema_name("table"), name, "table");
    -- TODO: inherit the rest

    -- Return the oid of the new table
    select c.oid from pg_class c join pg_namespace n on c.relnamespace = n.oid
    where (relname, nspname) = (name, @extschema@.schema_name("table"))
    into strict rv;
    return rv;
end
$$;

create function create_partition_update_trigger(partition regclass) returns void
language plpgsql as $f$
declare
    base_table regclass;
    field name;
    start_value text;
    end_value text;
    fname name;
    -- It should be the last of the triggers "before"
    -- But don't use a 'zzz' prefix as it clashes with pg_repack
    tname name = 'yyy_partition_update';
begin
    select t.field, p.start_value, p.end_value,
        -- Defined by create_update_function() in setup()
        @extschema@.table_name(p.base_table) || '_partition_update'
    from @extschema@.partition p
    join @extschema@.partitioned_table t on p.base_table = t."table"
    into field, start_value, end_value, fname;

    execute format($t$
        create trigger %I before update on %s
        for each row when (not (%L <= new.%I and new.%I < %L))
        execute procedure %s();
        $t$, tname, partition, start_value, field, field, end_value, fname);
end
$f$;

create function constraint_partition(partition regclass) returns void
language plpgsql as $f$
declare
    partname name := @extschema@.table_name(partition);
    field name;
    start_value text;
    end_value text;
begin
    select t.field, p.start_value, p.end_value
    from @extschema@.partition p
    join @extschema@.partitioned_table t on p.base_table = t."table"
    where p.partition = constraint_partition.partition
    into strict field, start_value, end_value;

    execute format(
        'alter table %I add constraint %I check (%L <= %I and %I < %L)',
        partition, partname || '_partition_chk',
        start_value, field, field, end_value);
end
$f$;

-- }}}

-- Virtual methods dispatch {{{

-- These are the 'virtual methods' of the methods defined by partition_schema.
-- They dispatch the call to the concrete methods defined in the
-- partition_schema records.

create function value2key(
    field_type regtype, schema_name name, params text[], value text)
returns text language plpgsql stable as $$
declare
    value2key text;
    rv text;
begin
    select s.value2key from @extschema@.partition_schema s
    where (s.field_type, s.name) = (value2key.field_type, value2key.schema_name)
    into strict value2key;

    execute 'select ' || value2key || '($1, $2::' || field_type || ')'
    into strict rv using params, value;
    return rv;
end
$$;

create function value2name(
    field_type regtype, schema_name name, params text[],
    value text, base_name name)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2name text;
    rv text;
begin
    select s.value2key, s.key2name from @extschema@.partition_schema s
    where (s.field_type, s.name)
        = (value2name.field_type, value2name.schema_name)
    into strict value2key, key2name;

    execute 'select ' || key2name
        || '($1, ' || value2key || '($1, $2::' || field_type || ')::text, $3)'
    into strict rv using params, value, base_name;
    return rv;
end
$$;

create function name_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@.value2name(
        cfg.field_type, cfg.schema_name, cfg.schema_params,
        value, relname)
    from pg_class r
    join @extschema@.partitioned_table cfg on r.oid = cfg."table"
    where cfg."table" = name_for."table";
$$;


create function value2start(
    field_type regtype, schema_name name, params text[], value text)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2start text;
    rv text;
begin
    select s.value2key, s.key2start from @extschema@.partition_schema s
    where (s.field_type, s.name)
        = (value2start.field_type, value2start.schema_name)
    into strict value2key, key2start;

    execute 'select ' || key2start
        || '($1, ' || value2key || '($1, $2::' || field_type || ')::text)'
    into strict rv using params, value;
    return rv;
end
$$;

create function start_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@.value2start(
        cfg.field_type, cfg.schema_name, cfg.schema_params, value)
    from pg_class r
    join @extschema@.partitioned_table cfg on r.oid = cfg."table"
    where cfg."table" = start_for."table";
$$;


create function value2end(
    field_type regtype, schema_name name, params text[], value text)
returns name language plpgsql stable as $$
declare
    value2key text;
    key2end text;
    rv text;
begin
    select s.value2key, s.key2end from @extschema@.partition_schema s
    where (s.field_type, s.name)
        = (value2end.field_type, value2end.schema_name)
    into strict value2key, key2end;

    execute 'select ' || key2end
        || '($1, ' || value2key || '($1, $2::' || field_type || ')::text)'
    into strict rv using params, value;
    return rv;
end
$$;

create function end_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@.value2end(
        cfg.field_type, cfg.schema_name, cfg.schema_params, value)
    from pg_class r
    join @extschema@.partitioned_table cfg on r.oid = cfg."table"
    where cfg."table" = end_for."table";
$$;

-- }}}

-- Partitioning schemas implementations {{{

create function month2key(params text[], value date) returns int
language sql stable as
$$
    select ((12 * date_part('year', $2) + date_part('month', $2) - 1)::int
        / params[1]::int) * params[1]::int;
$$;

create function month2start(params text[], key text) returns date
language sql stable as
$$
    select ('0001-01-01'::date
        + '1 month'::interval * key::int
        - '1 year'::interval)::date;
$$;

create function month2end(params text[], key text) returns date
language sql stable as $$
    select (@extschema@.month2start(params, key)
        + '1 month'::interval * params[1]::int)::date;
$$;

create function month2name(params text[], key text, base_name name) returns name
language sql stable as
$$
    select (base_name || '_'
        || to_char(@extschema@.month2start(params, key), 'YYYYMM'))::name;
$$;

insert into partition_schema values (
    'date'::regtype, 'monthly', '{months_per_partiton}',
    '@extschema@.month2key', '@extschema@.month2name',
    '@extschema@.month2start', '@extschema@.month2end');

-- }}}
