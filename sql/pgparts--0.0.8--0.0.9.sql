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

-- Can't add a value to an enum in a transaction
drop function info(regclass, text);

drop type partition_info;
drop type partition_state;

create type partition_state as
    enum ('unpartitioned', 'missing', 'present', 'detached', 'archived');

create type partition_info as (
    state @extschema@.partition_state,
    partition regclass);

create or replace function
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


create or replace function
_partitions("table" regclass) returns setof regclass
language sql as
$$
    select p.partition
    from @extschema@.existing_partition p
    where p.base_table = "table"
    and p.partition in (select @extschema@._children("table"));
$$;


create or replace function
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
        perform @extschema@._no_inherit(partition, "table");
        perform @extschema@.maintain_insert_function("table");
    end if;

    return partition;
end
$$;

create or replace function
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
archive_before("table" regclass, ts timestamptz) returns setof regclass
language plpgsql as
$$
declare
    archive regclass = @extschema@._archive_table("table");
    part regclass;
begin
    if archive is null then
        raise using
            message = format('archive table for %s not found', "table"),
            hint = format(
                'You should run "@extschema@.create_archive(%s)" before.',
                "table");
    end if;

    for part in
        select p.partition from @extschema@.existing_partition p
        where p.partition in (select @extschema@._partitions("table"))
        and p.end_value::timestamptz < ts
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
        raise 'The table % is not an archived partition', part;
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
