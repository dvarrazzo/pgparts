\pset format unaligned
\pset footer off
create schema partest;
set datestyle = 'ISO';

create extension pgparts with schema partest;

create table sometbl (
    id serial primary key,
    day date not null,
    data text);

select * from partest.info('sometbl', '2014-09-15');

select partest.create_for('sometbl', '2014-09-15');

-- We don't know this schema
select partest.setup('sometbl', 'day', 'derp');

select partest.setup('sometbl', 'day', 'monthly', '{{nmonths,3}}');
select * from partest.info('sometbl', '2014-09-15');

-- Setup works once
select partest.setup('sometbl', 'day', 'monthly', '{{nmonths,3}}');

-- This insert fails
insert into sometbl values (100, '2014-09-15', 'first');
select * from only sometbl;

select partest.create_for('sometbl', '2014-09-15');
select * from partest.info('sometbl', '2014-09-15');

-- Insert
insert into sometbl values (100, '2014-09-15', 'first');
select * from only sometbl;
select * from only sometbl_201407;

-- Update, no partition change
update sometbl set day = '2014-8-15' where id = 100;
select * from only sometbl;
select * from only sometbl_201407;

-- Update to fail partition
update sometbl set day = '2014-10-15' where id = 100;
select * from only sometbl;
select * from only sometbl_201407;

-- Create the missing partition and try again
select partest.create_for('sometbl', '2014-10-15');
update sometbl set day = '2014-10-15' where id = 100;
select * from only sometbl;
select * from only sometbl_201407;
select * from only sometbl_201410;

-- Detach the partition removes it from writing
insert into sometbl values (104, '2014-07-10', 'third');
select partest.detach_for('sometbl', '2014-07-10');
-- Partition is there but removed from the base table
select * from sometbl where day = '2014-07-10';
select * from sometbl_201407 where day = '2014-07-10';
-- Trigger has been maintained
insert into sometbl values (105, '2014-07-10', 'fourth');
insert into sometbl values (106, '2014-10-10', 'fifth');
-- Idempotent
select partest.detach_for('sometbl', '2014-07-10');
-- Can't create the same partition again
select partest.create_for('sometbl', '2014-07-10');
-- But can attach it back
select partest.attach_for('sometbl', '2014-07-10');
insert into sometbl values (105, '2014-07-10', 'fourth');
select * from sometbl where day = '2014-07-10' order by id;
-- Idempotent
select partest.attach_for('sometbl', '2014-07-10');

-- Constraints, indexes, options
create table constr1 (id1 int, id2 int, primary key (id1, id2));
insert into constr1 values (1,2), (3,4);
create table constr2 (
    id serial primary key,
    date date not null,
    fid1 int, fid2 int,
    foreign key (fid1, fid2) references constr1 (id1, id2),
    uint int check (uint > 0),
    unique (uint),
    iint int,
    c circle,
    exclude using gist (c with &&)
) with (autovacuum_enabled = true, fillfactor = 23);

alter index constr2_pkey set (fillfactor = 42);

create index constr2_iint on constr2(iint) with (fillfactor = 69);
create unique index somename on constr2(iint) where id > 0;
create unique index taken on constr2(iint) where id > 1;
create table constr2_201409_taken ();

select partest.setup('constr2', 'date', 'monthly', '{{nmonths,1}}');
select partest.create_for('constr2', '2014-09-01');

select conname, pg_get_constraintdef(oid, true) from pg_constraint
where conrelid = 'constr2_201409'::regclass
order by conname;

select pg_get_indexdef(indexrelid) from pg_index
where indrelid = 'constr2_201409'::regclass
order by 1;

select unnest(reloptions) from pg_class
where oid = 'constr2_201409'::regclass
order by 1;

-- Ownership
create user u1;
create user u2;
create user u3;
create schema testown;
grant create, usage on schema testown to public;
set session authorization u1;
create table testown.t1 (id int primary key, date date);
grant insert on table testown.t1 to u2;
grant select on table testown.t1 to u2 with grant option;
create table testown.t2 (id int primary key, date date);
revoke truncate on table testown.t2 from u1;
reset session authorization;
set session authorization u2;
grant select on table testown.t1 to u3;
grant select on table testown.t1 to public;
create table testown.t3 (id int primary key, date date);
reset session authorization;
alter table testown.t3 owner to u3;

create or replace function comp_acls(src regclass, tgt regclass,
   out src_acl aclitem, out tgt_acl aclitem, out match bool)
returns setof record
language sql as
$$
    with src as (select unnest(relacl) as src_acl from pg_class
        where oid = $1
        order by 1::text),
    tgt as (select unnest(relacl) as tgt_acl from pg_class
        where oid = $2
        order by 1::text)
    select *, src_acl = tgt_acl
    from src full outer join tgt on src_acl = tgt_acl
    order by 1::text;
$$;

select partest.setup('testown.t1', 'date', 'monthly');
select partest.create_for('testown.t1', '2014-09-01');
-- Note: failure to restore grantor because of issue #1
select * from comp_acls('testown.t1', 'testown.t1_201409');
select usename from pg_user u join pg_class c on c.relowner = u.usesysid
where c.oid = 'testown.t1'::regclass;

select partest.setup('testown.t2', 'date', 'monthly');
select partest.create_for('testown.t2', '2014-09-01');
select * from comp_acls('testown.t2', 'testown.t2_201409');
select usename from pg_user u join pg_class c on c.relowner = u.usesysid
where c.oid = 'testown.t1'::regclass;

select partest.setup('testown.t3', 'date', 'monthly');
select partest.create_for('testown.t3', '2014-09-01');
select * from comp_acls('testown.t3', 'testown.t3_201409');
select usename from pg_user u join pg_class c on c.relowner = u.usesysid
where c.oid = 'testown.t1'::regclass;

-- Test it works in security definer functions (issue #1)
create function testown.sdf() returns void language plpgsql security definer as
$$
begin
	perform partest.create_for('testown.t1', '2014-10-01');
	perform partest.create_for('testown.t2', '2014-10-01');
	perform partest.create_for('testown.t3', '2014-10-01');
end
$$;
grant all on function testown.sdf() to public;

set session authorization u3;
select testown.sdf();
reset session authorization;

set client_min_messages to 'error';
drop schema testown cascade;
reset client_min_messages;
drop user u1;
drop user u2;
drop user u3;

-- Monthly timestamp
create table monthts (id serial primary key, ts timestamp);
select partest.setup('monthts', 'ts', 'monthly');
select partest.create_for('monthts', '2014-09-01');
select partest.create_for('monthts', '2014-10-01');
insert into monthts(ts) values ('2014-08-31T23:59:59.999');
insert into monthts(ts) values ('2014-09-01');
insert into monthts(ts) values ('2014-09-30T23:59:59.999');
insert into monthts(ts) values ('2014-10-01');
insert into monthts(ts) values ('2014-10-31T23:59:59.999');
insert into monthts(ts) values ('2014-11-01');

-- Monthly timestamptz
create table monthtstz (id serial primary key, ts timestamptz);
select partest.setup('monthtstz', 'ts', 'monthly');
select partest.create_for('monthtstz', '2014-09-01');
select partest.create_for('monthtstz', '2014-10-01');
insert into monthtstz(ts) values ('2014-08-31T23:59:59.999Z');
insert into monthtstz(ts) values ('2014-09-01Z');
insert into monthtstz(ts) values ('2014-09-30T23:59:59.999Z');
insert into monthtstz(ts) values ('2014-10-01Z');
insert into monthtstz(ts) values ('2014-10-31T23:59:59.999Z');
insert into monthtstz(ts) values ('2014-11-01Z');

-- Daily timestamptz
create table days (id serial primary key, ts timestamptz);
select partest.setup('days', 'ts', 'daily', '{{ndays,1}}');
select partest.create_for('days', '2014-09-01');
select partest.create_for('days', '2014-09-02');
insert into days(ts) values ('2014-08-31T23:59:59.999Z');
insert into days(ts) values ('2014-09-01Z');
insert into days(ts) values ('2014-09-01T23:59:59.999Z');
insert into days(ts) values ('2014-09-02Z');
insert into days(ts) values ('2014-09-02T23:59:59.999Z');
insert into days(ts) values ('2014-09-03Z');

-- Weeks starting on Saturdays
create table weeks (id serial primary key, ts date);
select partest.setup('weeks', 'ts', 'daily',
    '{{ndays,7},{start_dow,6}}');
select partest.create_for('weeks', '2014-09-12');
select partest.create_for('weeks', '2014-09-13');
insert into weeks(ts) values ('2014-09-05');
insert into weeks(ts) values ('2014-09-06');
insert into weeks(ts) values ('2014-09-12');
insert into weeks(ts) values ('2014-09-13');
insert into weeks(ts) values ('2014-09-19');
insert into weeks(ts) values ('2014-09-20');

drop extension pgparts;
