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
