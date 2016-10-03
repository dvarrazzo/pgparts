create or replace function
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
