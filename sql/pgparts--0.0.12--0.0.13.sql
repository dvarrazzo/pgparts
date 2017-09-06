create function
_table_literal("table" regclass) returns text
language sql stable as
$$
    select format('%L', format('%I.%I', nspname, relname))
    from pg_class c
    join pg_namespace n on n.oid = relnamespace
    where c.oid = $1;
$$;


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
            @extschema@.name_for(%s::regclass, new.%I::text), new.%I),
        hint = format(
            $m$You should call @extschema@.create_for(%L, %%L).$m$, new.%I);
end
$$
$f$,
        schema, fname, null_check, checks, old_check,
        schema, field, @extschema@._table_literal("table"), field, field,
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
        raise invalid_parameter_value using
            message = $m$the field %I cannot be 'empty'$m$;
    end if;

    raise undefined_table using
        message = format(
            $m$partition %I.%%I missing for %I = %%L$m$,
            @extschema@.name_for(%s::regclass, lower(new.%I)::text), new.%I),
        hint = format(
            $m$You should call @extschema@.create_for(%L, %%L).$m$,
            lower(new.%I));
end
$$
$f$,
        schema, fname, type, null_check, checks,
        field, field,
        schema, field, @extschema@._table_literal("table"), field, field,
        "table", field);
end
$body$;
