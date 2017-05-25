--
-- pgparts -- simple tables partitioning for PostgreSQL
--
-- Copyright (C) 2014 Daniele Varrazzo <daniele.varrazzo@gmail.com>
--

-- Parameters handling {{{

create domain params as text[][];

create function
_param_value(params params, name name) returns text
language plpgsql immutable strict as
$$
declare
    param text[];
begin
    foreach param slice 1 in array params::text[][] loop
        if name = param[1] then
            return param[2];
        end if;
    end loop;
    return null;
end
$$;

create function
_param_exists(params params, name name) returns bool
language plpgsql immutable strict as
$$
declare
    param text[];
begin
    if array_ndims(params) is null then
        return false;
    end if;

    foreach param slice 1 in array params::text[][] loop
        if name = param[1] then
            return true;
        end if;
    end loop;

    return false;
end
$$;

-- }}}

-- Tables used to memorize partitioned tables {{{

create table partition_schema (
    name name,
    primary key (name),

    description text
);

comment on table partition_schema is
    'The partitioning schemas the system knows';

grant select on partition_schema to public;


create table schema_param (
    schema name,
    param name,
    primary key (schema, param),
    type regtype not null,
    "default" text,
    description text
);

comment on table partition_schema is
    'Parameter definitions of partitioning schemas';

grant select on schema_param to public;


create function
_valid_for_type(s text, t regtype) returns bool
language plpgsql immutable as $$
begin
    if t is not null then
        execute format('select %L::%s', s, t);
    end if;
    return true;
end
$$;

alter table schema_param add constraint valid_default
    check (_valid_for_type("default", type));

-- TODO: allow user-defined partition schemas to be dumped


create table _schema_vtable (
    schema_name name,
    foreign key (schema_name)
        references partition_schema (name)
        on update cascade on delete cascade,
    field_type regtype,
    primary key (schema_name, field_type),

    -- TODO: implement other partitioning strategies, not only by range:
    -- at least by identity. By hash is not worth: partitions are good to
    -- rotate stuff away, hashes to distribute. You better use PL/Proxy.

    value2key name not null,
    key2name name not null,
    key2start name not null,
    key2end name not null
);

grant select on _schema_vtable to public;


create table partitioned_table (
    "table" regclass primary key,
    field name not null,

    field_type regtype not null,
    schema_name text not null,
    foreign key (field_type, schema_name)
        references _schema_vtable (field_type, schema_name),

    schema_params params not null
);

comment on table partitioned_table is
    'The base tables prepared for partitioning';

-- Include in pg_dump
select pg_catalog.pg_extension_config_dump('partitioned_table', '');

grant select on partitioned_table to public;


create table partition (
    schema_name name,
    table_name name,
    primary key (schema_name, table_name),
    base_table regclass not null references partitioned_table ("table"),
    start_value text not null,
    end_value text not null);

comment on table partition is
    'The partition tables generated with the range covered by each of them';

select pg_catalog.pg_extension_config_dump('partition', '');

grant select on partition to public;


create view existing_partition as
select c.oid::regclass as partition,
    p.schema_name, p.table_name, p.base_table, p.start_value, p.end_value
    from pg_class c join pg_namespace s on c.relnamespace = s.oid
    join @extschema@.partition p
        on p.schema_name = s.nspname and p.table_name = c.relname;

comment on view existing_partition is
    'The partition tables that have not been dropped from the database';

grant select on existing_partition to public;


-- }}}

-- Virtual methods dispatch {{{

create function
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

    execute 'select ' || value2key || '($1, $2::' || field_type || ')'
    into strict rv using params, value;
    return rv;
end
$$;

create function
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

    execute 'select ' || key2name
        || '($1, ' || value2key || '($1, $2::'
        || @extschema@._base_type(field_type) || '), $3)'
    into strict rv using params, value, base_name;
    return rv;
end
$$;

create function
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

    execute 'select ' || key2start
        || '($1, ' || value2key || '($1, $2::'
        || @extschema@._base_type(field_type) || '))'
    into strict rv using params, value;
    return rv;
end
$$;

create function
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

    execute 'select ' || key2end
        || '($1, ' || value2key || '($1, $2::'
        || @extschema@._base_type(field_type) || '))'
    into strict rv using params, value;
    return rv;
end
$$;


create function
_null_predicate("table" regclass, prefix text default '') returns text
language sql stable as
$f$
    select format('%s%I is null', prefix, t.field)
    from @extschema@.partitioned_table t
    where t."table" = $1
$f$;

create function
_scalar_predicate(partition regclass, prefix text default '') returns text
language sql stable as
$f$
    select format('%L <= %s%I and %s%I < %L',
        p.start_value, prefix, t.field, prefix, t.field, p.end_value)
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on t."table" = p.base_table
    where p.partition = $1
$f$;

create function
_range_predicate(partition regclass, prefix text default '') returns text
language sql stable as
$f$
    select format($$%s%I <@ '[%s,%s)'$$,
        prefix, t.field, p.start_value, p.end_value)
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on t."table" = p.base_table
    where p.partition = $1
$f$;

create function
_check_predicate(partition regclass, prefix text default '') returns text
language plpgsql as $$
declare
    fname text;
    rv text;
begin
    if @extschema@._is_range(@extschema@._partition_field_type(partition)) then
        return @extschema@._range_predicate(partition, prefix);
    else
        return @extschema@._scalar_predicate(partition, prefix);
    end if;
end
$$;


-- }}}

-- Informative functions {{{

create function
_table_oid(schema name, name name) returns regclass
language sql stable strict as
$$
    select c.oid
    from pg_class c
    join pg_namespace n on n.oid = relnamespace
    where nspname = $1 and relname = $2;
$$;

create function
_table_name("table" regclass) returns name
language sql stable as
$$
    select relname from pg_class where oid = $1;
$$;

create function
_schema_name("table" regclass) returns name
language sql stable as
$$
    select nspname
    from pg_class c
    join pg_namespace n on n.oid = relnamespace
    where c.oid = $1;
$$;

create function
_owner_name("table" regclass) returns name
language sql stable as
$$
    select usename
    from pg_class c
    join pg_user u on relowner = usesysid
    where c.oid = $1;
$$;

create function
_has_pkey("table" regclass) returns bool
language sql stable as
$$
    select exists (
        select 1 from pg_constraint c
        where conrelid = "table"
        and contype = 'p');
$$;


create function
_is_range(type regtype) returns boolean
language plpgsql stable as
$$
begin
    if current_setting('server_version_num')::int >= 90200 then
        return exists (select 1 from pg_range where rngtypid = type);
    else
        return false;
    end if;
end
$$;

create function
_base_type(type regtype) returns regtype
language plpgsql stable as
$$
declare
    rv regtype;
begin
    if current_setting('server_version_num')::int >= 90200 then
        select rngsubtype::regtype from pg_range
        where rngtypid = type
        into rv;
        if found then
            return rv;
        end if;
    end if;

    return type;
end
$$;

comment on function _base_type(regtype) is
$$If the input type is a range, return its base type, else return the input.$$;


create function
name_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2name(
        cfg.field_type, cfg.schema_name, cfg.schema_params,
        value, relname)
    from pg_class r
    join @extschema@.partitioned_table cfg on r.oid = cfg."table"
    where cfg."table" = name_for."table";
$$;

create function
start_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2start(
        cfg.field_type, cfg.schema_name, cfg.schema_params, value)
    from pg_class r
    join @extschema@.partitioned_table cfg on r.oid = cfg."table"
    where cfg."table" = start_for."table";
$$;

create function
end_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2end(
        cfg.field_type, cfg.schema_name, cfg.schema_params, value)
    from pg_class r
    join @extschema@.partitioned_table cfg on r.oid = cfg."table"
    where cfg."table" = end_for."table";
$$;

create function
partition_for("table" regclass, value text) returns regclass
language sql as
$$
    select @extschema@._table_oid(
        @extschema@._schema_name("table"),
        @extschema@.name_for("table", value));
$$;


create type partition_state as
    enum ('unpartitioned', 'missing', 'present', 'detached');

create type partition_info as (
    state @extschema@.partition_state,
    partition regclass);

create function
info("table" regclass, value text)
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

create function
_partitions("table" regclass) returns setof regclass
language sql as
$$
    select p.partition
    from @extschema@.existing_partition p
    join pg_inherits i on p.partition = inhrelid
    where p.base_table = "table" and inhparent = "table";
$$;


create function
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

    raise 'the table % is not a partitioned table or a partition', t;
end
$$;

comment on function _partition_field_type(regclass) is
$$Return the type of the partitioning field for a table.

The input table can be either a partitioned table or a partition.
$$;


create function
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

    raise 'the table % is not a partitioned table or a partition', t;
end
$$;

comment on function _partition_field_nullable(regclass) is
$$Return 'true' if the partition field is nullable.

Records with null value in the partition fields are stored in the base table.

The input table can be either a partitioned table or a partition.
$$;


-- }}}

-- Setting up a partitioned table {{{

create function
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
            raise using
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
            raise 'field % not found in table %', field, "table";
        end if;

        -- Does this partitioning schema exist?
        perform 1 from @extschema@._schema_vtable v
            where (v.field_type, v.schema_name)
                = (block.field_type, setup.schema_name);
        if not found then
            raise 'partitioning schema % on type % not known',
                schema_name, field_type;
        end if;

        -- Validate the schema parameters
        if array_ndims(schema_params::text[][]) is not null then
            foreach param slice 1 in array schema_params::text[][] loop
                select type from @extschema@.schema_param sp
                where sp.schema = schema_name and sp.param = block.param[1]
                into param_type;
                if not found then
                    raise 'unknown parameter for partitioning schema %: %',
                        schema_name, param[1];
                end if;
                perform @extschema@._valid_for_type(block.param[2], param_type);
            end loop;
        end if;

        -- Complete the missing parameters with defaults
        for param_name, param_default in
        select sp.param, sp."default" from @extschema@.schema_param sp
        where sp.schema = schema_name loop
            if not @extschema@._param_exists(schema_params, param_name) then
                schema_params := schema_params
                    || array[array[param_name::text, param_default]];
            end if;
        end loop;

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

create function
_register_partitioned_table(
    "table" regclass, field name, field_type regtype,
    schema_name text, schema_params params) returns void
language plpgsql security definer as
$$
begin
    insert into @extschema@.partitioned_table
        ("table", field, field_type, schema_name, schema_params)
    values
        ("table", field, field_type, schema_name, schema_params);
end
$$;


-- TODO: implement bisection access (should be a partitioned table param)
create function
maintain_insert_function("table" regclass) returns void
language plpgsql as $$
declare
    nparts int;
begin
    select count(*) from @extschema@._partitions("table")
    into nparts;

    if nparts = 0 then
        perform @extschema@._maintain_insert_function_empty("table");
    else
        if @extschema@._is_range(@extschema@._partition_field_type("table")) then
            perform @extschema@._maintain_insert_function_range("table");
        else
            perform @extschema@._maintain_insert_function_scalar("table");
        end if;
    end if;
end
$$;


create function
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
    raise using
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
_null_insert_snippet("table" regclass) returns text
language sql stable as
$f$
    select case when @extschema@._partition_field_nullable("table") then
        format(
$$
    if %s then
        return new;
    end if;
$$,
        @extschema@._null_predicate("table", 'new.'))
    else
        ''
    end;
$f$;


create function
_scalar_insert_snippet(partition regclass) returns text
language sql stable as
$f$
    select format(
$$
    if %s then
        insert into %I.%I values (new.*);
        return null;
    end if;
$$,
        @extschema@._scalar_predicate(p.partition, 'new.'),
        p.schema_name, p.table_name)
    from @extschema@.existing_partition p
    where p.partition = $1
$f$;


create function
_maintain_insert_function_scalar("table" regclass) returns void
language plpgsql as $body$
declare
    schema name = @extschema@._schema_name("table");
    fname name = @extschema@._table_name("table") || '_partition_insert';
    field name;
    null_check text;
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

    execute format(
$f$
create or replace function %I.%I()
returns trigger language plpgsql as $$
begin
%s%s
    raise using
        message = format(
            $m$partition %I.%%I missing for %I = %%L$m$,
            @extschema@.name_for(%L::regclass, new.%I::text), new.%I),
        hint = format(
            $m$You should call @extschema@.create_for(%L, %%L).$m$, new.%I);
end
$$
$f$,
        schema, fname, null_check, checks,
        schema, field, "table", field, field,
        "table", field);
end
$body$;


create function
_range_insert_snippet(partition regclass) returns text
language sql stable as
$f$
    select format(
$$
    if %L && new.%I then
        if %L @> new.%I then
            insert into %I.%I values (new.*);
            return null;
        else
            rest = new.%I - %L;
            new.%I = new.%I * %L;
            insert into %I.%I values (new.*);
            new.%I = rest;
        end if;
    end if;
$$,
        range, field, range, field,         -- if, if
        schema_name, table_name,            -- insert into
        field, range, field, field, range,  -- rest =, new.x =
        schema_name, table_name, field)     -- insert into
    from (select
        t.field, p.schema_name, p.table_name,
            format('[%s,%s)', p.start_value, p.end_value) as range
        from @extschema@.existing_partition p
        join @extschema@.partitioned_table t on t."table" = p.base_table
        where p.partition = $1) x
$f$;

-- TODO: if a partition is missing the error message is not very informative:
-- "result of range difference would not be contiguous"
create function
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
        raise $m$the field %I cannot be 'empty'$m$;
    end if;

    raise using
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


create function
_create_insert_trigger("table" regclass) returns void
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

create function
_create_update_trigger("table" regclass) returns void
language plpgsql as
$f$
declare
    sname name = @extschema@._schema_name("table");
    fname name = @extschema@._table_name("table") || '_partition_update';
    tname name = 'yyy_partition_update';
begin
    if not @extschema@._partition_field_nullable("table") then
        return;
    end if;

    execute format($t$
        create trigger %I before update on %s
        for each row when (not %s)
        execute procedure %I.%I();
        $t$,
        tname, "table",
        @extschema@._null_predicate("table", 'new.'),
        sname, fname);
end
$f$;

-- The function created is used by triggers on the partitions,
-- not on the base table. The table must have a primary key.
create function
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
        create function %I.%I() returns trigger language plpgsql as $$
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


-- }}}

-- Setting up a partition {{{

create function
_cast(value text, type regtype) returns text
language plpgsql immutable strict as
$$
declare
    rv text;
begin
    execute format('select %L::%s::text', value, type)
    into strict rv;
    return rv;
end
$$;

create function
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

create function
_register_partition(
    schema_name name, table_name name, base_table regclass,
    start_value text, end_value text) returns void
language plpgsql security definer as
$$
begin
    insert into @extschema@.partition
        (schema_name, table_name, base_table, start_value, end_value)
    values
        (schema_name, table_name, base_table, start_value, end_value);
end
$$;

create function
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

create function
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


create function
_copy_to_subtable("table" regclass, value text) returns regclass
language plpgsql as
$$
declare
    name name = @extschema@.name_for("table", value);
    partition regclass;
begin
    execute format ('create table %I.%I () inherits (%s)',
        @extschema@._schema_name("table"), name, "table");

    partition = @extschema@.partition_for("table", value);
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

create function
_create_partition_update_trigger(partition regclass) returns void
language plpgsql as
$f$
declare
    base_table regclass;
    fname name;
    sname name;
    -- It should be the last of the triggers "before"
    -- But don't use a 'zzz' prefix as it clashes with pg_repack
    tname name = 'yyy_partition_update';
    check_null text = '';
begin
    select
        -- Defined by _create_update_function() in setup()
        @extschema@._table_name(p.base_table) || '_partition_update',
        @extschema@._schema_name(p.base_table),
        p.base_table
    from @extschema@.existing_partition p
    where p.partition = _create_partition_update_trigger.partition
    into strict fname, sname, base_table;

    if @extschema@._partition_field_nullable(partition) then
        check_null = format(
            '(%s) or',
            @extschema@._null_predicate(base_table, 'new.'));
    end if;

    execute format($t$
        create trigger %I before update on %s
        for each row when (%s not (%s))
        execute procedure %I.%I();
        $t$,
        tname, partition,
        check_null,
        @extschema@._check_predicate(partition, 'new.'),
        sname, fname);
end
$f$;

create function
_constraint_partition(partition regclass) returns void
language plpgsql as
$$
declare
    partname name := @extschema@._table_name(partition);
    field name;
    start_value text;
    end_value text;
begin
    select t.field, p.start_value, p.end_value
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on p.base_table = t."table"
    where p.partition = _constraint_partition.partition
    into strict field, start_value, end_value;

    execute format(
        'alter table %s add constraint %I check (%s)',
        partition, partname || '_partition_check',
        @extschema@._check_predicate(partition));
end
$$;


-- }}}

-- Partitions archiving {{{

create function
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
    execute format('create table %I.%I (like %I.%I)',
        sname, tname || '_archived', sname, tname);
    execute format('alter table %I.%I inherit %I.%I',
        sname, tname || '_archived', sname, tname || '_all');

    rv = @extschema@._archive_table("table");
    if rv is null then
        raise 'uhm, I should have created this table...';
    end if;

    return rv;
end
$$;


create function
_archive_table("table" regclass) returns regclass
language sql stable as
$$
    -- Return the archive table of a partitioned table; NULL if it doesn't exist
    select c.oid::regclass
    from pg_class c join pg_namespace n on n.oid = relnamespace
    where nspname = @extschema@._schema_name("table")
    and relname = @extschema@._table_name("table") || '_archived';
$$;


-- }}}

-- Partitioning schemas implementations {{{

create domain positive_integer as integer
    constraint greater_than_zero check (value > 0);

create domain day_of_week as integer
    constraint valid_dow check (0 <= value and value <= 6);

create domain timezone as text
    constraint valid_timezone check
        ((('2014-01-01'::timestamp) at time zone value) is not null);

insert into partition_schema values ('monthly',
$$Each partition of the table contains one or more months.

The partitioning triggers checks the partitions from the newest to the oldest
so, if normal inserts happens in order of time, dispatching to the right
partition should be o(1), whereas for random inserts dispatching is o(n) in the
number of partitions.
$$);

insert into schema_param values (
    'monthly', 'nmonths', '@extschema@.positive_integer', '1',
    'Number of months contained in each partition.');

insert into schema_param values (
    'monthly', 'timezone', '@extschema@.timezone', 'UTC',
$$The time zone of the partitions boundaries.

Only used if the partitioned field type is timestamp with time zone.$$);


create function
_month2key(params params, value timestamptz) returns int
language sql stable as
$$
    select
        ((12 * (date_part('year', $2) - 1970)
            + date_part('month', $2) - 1)::int
        / @extschema@._param_value(params, 'nmonths')::int)
        * @extschema@._param_value(params, 'nmonths')::int;
$$;

create function
_month2start(params params, key int) returns date
language sql stable as
$$
    select ('epoch'::date + '1 month'::interval * key)::date;
$$;

create function
_month2end(params params, key int) returns date
language sql stable as $$
    select (@extschema@._month2start(params, key)
        + '1 month'::interval
        * @extschema@._param_value(params, 'nmonths')::int)::date;
$$;

create function
_month2name(params params, key int, base_name name)
returns name language sql stable as
$$
    select (base_name || '_'
        || to_char(@extschema@._month2start(params, key), 'YYYYMM'))::name;
$$;

insert into _schema_vtable values (
    'monthly', 'date'::regtype,
    '@extschema@._month2key', '@extschema@._month2name',
    '@extschema@._month2start', '@extschema@._month2end');

insert into _schema_vtable values (
    'monthly', 'timestamp'::regtype,
    '@extschema@._month2key', '@extschema@._month2name',
    '@extschema@._month2start', '@extschema@._month2end');


create function
_month2keytz(params params, value timestamptz) returns int
language sql stable as
$$
    select @extschema@._month2key(params,
        value at time zone @extschema@._param_value(params, 'timezone'));
$$;

create function
_month2starttz(params params, key int) returns timestamptz
language sql stable as
$$
    select @extschema@._month2start(params, key)::timestamp
        at time zone @extschema@._param_value(params, 'timezone');
$$;

create function
_month2endtz(params params, key int) returns timestamptz
language sql stable as $$
    select @extschema@._month2end(params, key)::timestamp
        at time zone @extschema@._param_value(params, 'timezone');
$$;

insert into _schema_vtable values (
    'monthly', 'timestamptz'::regtype,
    '@extschema@._month2keytz', '@extschema@._month2name',
    '@extschema@._month2starttz', '@extschema@._month2endtz');

insert into _schema_vtable values (
    'monthly', 'tstzrange'::regtype,
    '@extschema@._month2keytz', '@extschema@._month2name',
    '@extschema@._month2starttz', '@extschema@._month2endtz');


insert into partition_schema values ('daily',
$$Each partition of the table contains one or more days.

The partitioning triggers checks the partitions from the newest to the oldest
so, if normal inserts happens in order of time, dispatching to the right
partition should be o(1), whereas for random inserts dispatching is o(n) in the
number of partitions.
$$);

insert into schema_param values (
    'daily', 'ndays', '@extschema@.positive_integer', '1',
    'Number of days contained in each partition.');

insert into schema_param values (
    'daily', 'start_dow', '@extschema@.day_of_week', '0',
$$Day of the week each partition starts if 'ndays' = 7.

Consistently with extract('dow' from date), 0 is Sunday, 6 is Saturday.
$$);

insert into schema_param values (
    'daily', 'timezone', '@extschema@.timezone', 'UTC',
$$The time zone of the partitions boundaries.

Only used if the partitioned field type is timestamp with time zone.$$);


create function
_day2key(params params, value timestamptz) returns int
language sql stable as
$$
    -- The 3 makes weekly partitions starting on Sunday with offset 0
    -- which is consistent with extract(dow), if anything.
    select
        (((value::date - 'epoch'::date)::int
            - @extschema@._param_value(params, 'start_dow')::int - 3)
        / @extschema@._param_value(params, 'ndays')::int)
        * @extschema@._param_value(params, 'ndays')::int;
$$;

create function
_day2start(params params, key int) returns date
language sql stable as
$$
    select ('epoch'::date + '1 day'::interval * (key + 3
        + @extschema@._param_value(params, 'start_dow')::int))::date;
$$;

create function
_day2end(params params, key int) returns date
language sql stable as $$
    select (@extschema@._day2start(params, key)
        + '1 day'::interval
        * @extschema@._param_value(params, 'ndays')::int)::date;
$$;

create function
_day2name(params params, key int, base_name name)
returns name language sql stable as
$$
    select (base_name || '_'
        || to_char(@extschema@._day2start(params, key), 'YYYYMMDD'))::name;
$$;

insert into _schema_vtable values (
    'daily', 'date'::regtype,
    '@extschema@._day2key', '@extschema@._day2name',
    '@extschema@._day2start', '@extschema@._day2end');

insert into _schema_vtable values (
    'daily', 'timestamp'::regtype,
    '@extschema@._day2key', '@extschema@._day2name',
    '@extschema@._day2start', '@extschema@._day2end');


create function
_day2keytz(params params, value timestamptz) returns int
language sql stable as
$$
    select @extschema@._day2key(params,
        value at time zone @extschema@._param_value(params, 'timezone'));
$$;

create function
_day2starttz(params params, key int) returns timestamptz
language sql stable as
$$
    select @extschema@._day2start(params, key)::timestamp
        at time zone @extschema@._param_value(params, 'timezone');
$$;

create function
_day2endtz(params params, key int) returns timestamptz
language sql stable as $$
    select @extschema@._day2end(params, key)::timestamp
        at time zone @extschema@._param_value(params, 'timezone');
$$;

insert into _schema_vtable values (
    'daily', 'timestamptz'::regtype,
    '@extschema@._day2keytz', '@extschema@._day2name',
    '@extschema@._day2starttz', '@extschema@._day2endtz');


-- }}}

-- Generic functions to copy tables properties {{{

-- These are useful enough to deserve to be publicly accessible
-- (maybe they deserve an extension of itself)

create function
make_unique_relname(schema name, name name) returns name
language plpgsql stable as $$
declare
    orig name = name;
    seq int = 0;
begin
    loop
        perform 1 from pg_class where relname = name;
        exit when not found;
        seq = seq + 1;
        name = orig || seq;
    end loop;
    return name;
end
$$;

create function
copy_constraints(src regclass, tgt regclass, exclude_types text[] default '{}')
returns void
language plpgsql as $$
declare
    indid oid;
    newindid oid;
    oldidxs oid[];
    csrcname text;
    ctgtname text;
    srcname name = @extschema@._table_name(src);
    tgtname name = @extschema@._table_name(tgt);
    condef text;
begin
    for indid, csrcname, condef in
        -- the conindid for fkeys is the referenced index, not a local one
        select case when contype <> 'f' then conindid else 0 end,
            conname, pg_get_constraintdef(oid)
        from pg_constraint
        where conrelid = src
        and contype <> any (exclude_types)
    loop
        if indid <> 0 then
            -- We don't know the name of the new constraint, so we have to
            -- look for a new index oid.
            select coalesce(array_agg(indexrelid), '{}') from pg_index
            where indrelid = tgt
            into strict oldidxs;
        end if;

        -- Try to respect the naming convention of the constraint if any.
        -- Otherwise let postgres make up a new one.
        if position(srcname in csrcname) > 0 then
            ctgtname = overlay(csrcname placing tgtname
                from position(srcname in csrcname)
                for length(srcname));
            execute format('alter table %s add constraint %I %s',
                tgt, ctgtname, condef);
        else
            execute format('alter table %s add %s', tgt, condef);
        end if;

        if indid <> 0 then
            select indexrelid from pg_index
            where indrelid = tgt
            and not indexrelid = any(oldidxs)
            into strict newindid;

            perform @extschema@.copy_options(indid, newindid);
        end if;
    end loop;
end
$$;

create function
copy_indexes(src regclass, tgt regclass) returns void
language plpgsql as $body$
declare
    isrcname name;
    itgtname name;
    indexdef text;
    srcname name = @extschema@._table_name(src);
    tgtname name = @extschema@._table_name(tgt);
    schema name = @extschema@._schema_name(tgt);
    parts text[];
begin
    for isrcname, indexdef in
        select ic.relname, pg_get_indexdef(i.indexrelid)
        from pg_index i
        join pg_class ic on ic.oid = i.indexrelid
        where indrelid = src
        and not exists (
            select 1 from pg_constraint
            where conrelid = src
            and conindid = indexrelid)
    loop
        -- Indexes require an unique name. Replace the src table name with the
        -- tgt if the table name is contained in the index name, else make up
        -- something.
        if position(srcname in isrcname) > 0 then
            itgtname = overlay(isrcname placing tgtname
                from position(srcname in isrcname)
                for length(srcname));
        else
            itgtname = tgtname || '_' || isrcname;
        end if;

        -- Make sure the new name is unique
        itgtname = @extschema@.make_unique_relname(schema, itgtname);

        -- Find the elements in the index definition.
        -- The 'strict' causes an error if the regexp fails to parse
        select regexp_matches(indexdef,
            '^(CREATE (?:UNIQUE )?INDEX )(.*)( ON )(.*)( USING .*)$')
        into strict parts;
        execute format('%s%I%s%s%s',
            parts[1], itgtname, parts[3], tgt, parts[5]);

        -- Copy the index options too
        perform @extschema@.copy_options(
            @extschema@._table_oid(schema, isrcname),
            @extschema@._table_oid(schema, itgtname));

    end loop;
end
$body$;

create function
copy_triggers(src regclass, tgt regclass) returns void
language plpgsql as $body$
declare
    tgdis bool;
    tgname name;
    tgdef text;
    parts text[];
begin

    -- Note: copy only the AFTER triggers; BEFORE triggers may be triggered
    -- too many times (e.g. by the base table and the partition table).
    -- this extension creates only BEFORE trigger so it doesn't copy its own.
    for tgname, tgdis, tgdef in
        select t.tgname, t.tgenabled = 'D', pg_get_triggerdef(t.oid)
        from pg_trigger t
        where t.tgrelid = src
        and not t.tgisinternal
        and t.tgtype & 2 = 0      -- AFTER trigger (see pg_trigger.h)
        and not t.tgname ~ '^_'   -- convention just made up, but skip londiste
    loop
        -- Find the elements in the trigger definition.
        -- The 'strict' causes an error if the regexp fails to parse
        select regexp_matches(tgdef,
            '^(.* ON )(.*)( FOR .*)$')
        into strict parts;
        execute format('%s%s%s', parts[1], tgt, parts[3]);

        if tgdis then
            execute format('ALTER TABLE %s DISABLE TRIGGER %I',
                tgt, tgname);
        end if;

    end loop;
end
$body$;

create function
copy_owner(src regclass, tgt regclass) returns void
language plpgsql as $$
declare
    osrc name = @extschema@._owner_name(src);
    otgt name = @extschema@._owner_name(tgt);
begin
    if osrc <> otgt then
        execute format('alter table %s owner to %I', tgt, osrc);
    end if;
end
$$;

create function
copy_permissions(src regclass, tgt regclass) returns void
language plpgsql as $$
declare
    grantee text;
    set_sess text;
    grant text;
    reset_sess text;
declare
    prev_grantee text = '';
begin
    for grantee, set_sess, grant, reset_sess in
        with acl as (
            select unnest(relacl) as acl
            from pg_class where oid = src),
        acl_token as (
            select regexp_matches(acl::text, '([^=]*)=([^/]*)(?:/(.*))?')
                as acl_group
            from acl),
        acl_bit as (
            select acl_group[1] as grantee,
                regexp_matches(acl_group[2], '(.)(\*?)', 'g') as bits,
                acl_group[3] as grantor
            from acl_token),
        bit_perm (bit, perm) as (values
            ('r', 'select'), ('w', 'update'), ('a', 'insert'), ('d', 'delete'),
            ('D', 'truncate'), ('x', 'references'), ('t', 'trigger')),
        pretty as (
            select case when b.grantee = '' then 'public' else b.grantee end
                as grantee,
            p.perm, b.bits[2] = '*' as grant_opt, b.grantor
            from acl_bit b left join bit_perm p on p.bit = b.bits[1])
        select
            p.grantee,
            case when current_user <> p.grantor then
                format('set role %s', p.grantor) end
                    as set_sess,
            format (
                'grant %s on table %s to %s%s', perm, tgt, p.grantee,
                case when grant_opt then ' with grant option' else '' end)
                    as grant,
            case when current_user <> p.grantor then
                'reset role'::text end as reset_sess
        from pretty p
    loop
        -- For each grantee, revoke all his roles and set them from scratch.
        -- This could have been done with a window function but the
        -- query is already complicated enough...
        if prev_grantee <> grantee then
            execute format('revoke all on %s from %s', tgt, grantee);
            prev_grantee = grantee;
        end if;
        -- Avoid trying to restore the grantor, as it will fail
        -- in security definer functions (issue #1)
        -- if set_sess is not null then
        --     execute set_sess;
        -- end if;
        execute "grant";
        -- if reset_sess is not null then
        --     execute reset_sess;
        -- end if;
    end loop;
end
$$;

create function
copy_options(src regclass, tgt regclass) returns void
language plpgsql as $$
declare
    kind text;
    opt text;
begin
    for kind, opt in
    select case
        when relkind = 'r' then 'table'
        when relkind = 'i' then 'index' end,
        unnest(reloptions)
    from pg_class
    where oid = src
    and relkind in ('r', 'i') loop
        execute format('alter %s %s set (%s)', kind, tgt, opt);
    end loop;
end;
$$;

-- }}}

-- Public utility functions {{{

create function
foreach_table(schema_name text, name_pattern text, statement text) returns void
language plpgsql as $$
-- Perform *statement* for each table matching the pattern
declare
    t regclass;
begin
    for t in
    select c.oid::regclass
    from pg_class c
    join pg_namespace n on n.oid = relnamespace
    where nspname = schema_name
    and relname ~ name_pattern
    and relkind = 'r'
    order by relname
    loop
        raise notice 'executing statement: %',
            format(statement, t, t, t, t, t);
        -- Can use up to 5 placeholders; extra values are discarded
        execute format(statement, t, t, t, t, t);
    end loop;
end
$$;


create function
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
            raise using
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


-- }}}

-- vi: set expandtab:
