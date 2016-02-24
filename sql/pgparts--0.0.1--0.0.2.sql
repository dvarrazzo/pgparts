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
    loop
        raise notice 'executing statement: %',
            format(statement, t, t, t, t, t);
        -- Can use up to 5 placeholders; extra values are discarded
        execute format(statement, t, t, t, t, t);
    end loop;
end
$$;
