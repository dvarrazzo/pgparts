create or replace function
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
