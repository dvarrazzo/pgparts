create function
_null_predicate("table" regclass, prefix text default '') returns text
language sql stable as
$f$
    select format('%s%I is null', prefix, t.field)
    from @extschema@.partitioned_table t
    where t."table" = $1
$f$;


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


create or replace function
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


create or replace function
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
