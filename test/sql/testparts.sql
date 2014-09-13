create schema partest;
set datestyle = 'ISO';

create extension pgparts with schema partest;

create table sometbl (
    id serial primary key,
    day date not null,
    data text);

select * from partest.info('sometbl', '2014-09-15');

select partest.create_for('sometbl', '2014-09-15');

select partest.setup('sometbl'::regclass, 'day', 'monthly', '{3}');
select * from partest.info('sometbl', '2014-09-15');

-- Setup works once
select partest.setup('sometbl'::regclass, 'day', 'monthly', '{3}');

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

-- Constraints and indexes
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
);

create index constr2_iint on constr2(iint);
create unique index somename on constr2(iint) where id > 0;
create unique index taken on constr2(iint) where id > 1;
create table constr2_201409_taken ();

select partest.setup('constr2', 'date', 'monthly', '{1}');
select partest.create_for('constr2', '2014-09-01');

select conname, pg_get_constraintdef(oid, true) from pg_constraint
where conrelid = 'constr2_201409'::regclass
order by conname;

select pg_get_indexdef(indexrelid) from pg_index
where indrelid = 'constr2_201409'::regclass
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

select partest.setup('testown.t1', 'date', 'monthly', '{1}');
select partest.create_for('testown.t1', '2014-09-01');
select * from comp_acls('testown.t1', 'testown.t1_201409');
select usename from pg_user u join pg_class c on c.relowner = u.usesysid
where c.oid = 'testown.t1'::regclass;

select partest.setup('testown.t2', 'date', 'monthly', '{1}');
select partest.create_for('testown.t2', '2014-09-01');
select * from comp_acls('testown.t2', 'testown.t2_201409');
select usename from pg_user u join pg_class c on c.relowner = u.usesysid
where c.oid = 'testown.t1'::regclass;

select partest.setup('testown.t3', 'date', 'monthly', '{1}');
select partest.create_for('testown.t3', '2014-09-01');
select * from comp_acls('testown.t3', 'testown.t3_201409');
select usename from pg_user u join pg_class c on c.relowner = u.usesysid
where c.oid = 'testown.t1'::regclass;

set client_min_messages to 'error';
drop schema testown cascade;
reset client_min_messages;
drop user u1;
drop user u2;
drop user u3;

drop extension pgparts;
