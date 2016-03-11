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

