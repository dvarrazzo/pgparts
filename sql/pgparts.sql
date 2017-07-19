--
-- pgparts -- simple tables partitioning for PostgreSQL
--
-- Copyright (C) 2014 Daniele Varrazzo <daniele.varrazzo@gmail.com>
--

create domain params as text[][];

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
    select format('%L::%I <= %s%I and %s%I < %L::%I',
        p.start_value, typname, prefix, t.field,
        prefix, t.field, p.end_value, typname)
    from @extschema@.existing_partition p
    join @extschema@.partitioned_table t on t."table" = p.base_table
    join pg_type on t.field_type = pg_type.oid
    where p.partition = $1
$f$;

create function
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
_pkey("table" regclass) returns name
language sql stable as
$$
    select conname from pg_constraint c
    where conrelid = "table"
    and contype = 'p';
$$;

create function
_has_pkey("table" regclass) returns bool
language sql stable as
$$
    select @extschema@._pkey("table") is not null;
$$;

create function
_children("table" regclass) returns setof regclass
language sql stable as
$$
    select inhrelid::regclass from pg_inherits
    where inhparent = "table";
$$;

create function
_parents("table" regclass) returns setof regclass
language sql stable as
$$
    select inhparent::regclass from pg_inherits
    where inhrelid = "table";
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


create function
name_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2name(name_for."table", value, relname)
    from pg_class r
    where r.oid = name_for."table";
$$;

create function
_name_for("table" regclass, start_value text, end_value text) returns name
language sql stable as
$$
    select @extschema@._values2name("table", start_value, end_value, relname)
    from pg_class r
    where r.oid = _name_for."table";
$$;

create function
start_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2start("table", value);
$$;

create function
end_for("table" regclass, value text) returns name
language sql stable as
$$
    select @extschema@._value2end("table", value);
$$;

create function
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

create type partition_state as
    enum ('unpartitioned', 'missing', 'present', 'detached', 'archived');

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
    rv.state = @extschema@._partition_state(rv.partition);
    return rv;
end
$$;


create function
_partition_state(part regclass) returns text
language plpgsql stable as
$$
declare
    parent regclass;
begin
    if part is null then
        return 'missing';
    end if;

    select base_table
    from @extschema@.existing_partition p
    where p.partition = part
    into parent;
    if not found then
        return null;
    end if;

    if part in (select @extschema@._children(parent)) then
        return 'present';
    elsif part in (select @extschema@._children(
            @extschema@._archive_table(parent))) then
        return 'archived';
    else
        return 'detached';
    end if;
end
$$;


create function
_partitions("table" regclass) returns setof regclass
language sql as
$$
    select p.partition
    from @extschema@.existing_partition p
    where p.base_table = "table"
    and p.partition in (select @extschema@._children("table"));
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

    raise object_not_in_prerequisite_state using
        message = format(
            'the table %s is not a partitioned table or a partition', t);
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

    raise object_not_in_prerequisite_state using
        message = format(
            'the table %s is not a partitioned table or a partition', t);
end
$$;

comment on function _partition_field_nullable(regclass) is
$$Return 'true' if the partition field is nullable.

Records with null value in the partition fields are stored in the base table.

The input table can be either a partitioned table or a partition.
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


create function
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


create function
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


create function
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


create function
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

create function
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

create function
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

create function
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


create function
_inherit(child regclass, parent regclass) returns void
language plpgsql as
$$
begin
    if child is null then raise 'child cannot be null'; end if;
    if parent is null then raise 'parent cannot be null'; end if;
    execute format('alter table %s inherit %s', child, parent);
end
$$;


create function
_no_inherit(child regclass, parent regclass) returns void
language plpgsql as
$$
begin
    if child is null then raise 'child cannot be null'; end if;
    if parent is null then raise 'parent cannot be null'; end if;
    execute format('alter table %s no inherit %s', child, parent);
end
$$;


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
        raise internal_error using
            message = 'uhm, I should have created this table...';
    end if;

    return rv;
end
$$;


create function
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


create function
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

insert into _schema_vtable values (
    'monthly', 'date'::regtype,
    '@extschema@._month2key', '@extschema@._month2name',
    '@extschema@._month2start', '@extschema@._month2end');

insert into _schema_vtable values (
    'monthly', 'timestamp'::regtype,
    '@extschema@._month2key', '@extschema@._month2name',
    '@extschema@._month2start', '@extschema@._month2end');


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

insert into _schema_vtable values (
    'daily', 'date'::regtype,
    '@extschema@._day2key', '@extschema@._day2name',
    '@extschema@._day2start', '@extschema@._day2end');

insert into _schema_vtable values (
    'daily', 'timestamp'::regtype,
    '@extschema@._day2key', '@extschema@._day2name',
    '@extschema@._day2start', '@extschema@._day2end');


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


-- }}}

-- vi: set expandtab:
