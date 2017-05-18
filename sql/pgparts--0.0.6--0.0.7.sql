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
            '^(.* ON )(.*)( (FOR|WHEN|EXECUTE) .*)$')
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
