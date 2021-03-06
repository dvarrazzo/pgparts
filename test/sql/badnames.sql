\pset format unaligned
\pset footer off
\set VERBOSITY terse
set datestyle = 'ISO';

create schema "s1.s2";
create extension pgparts with schema "s1.s2";

create schema "s3.s4";
create table "s3.s4"."t1.t2" (
    id serial primary key,
    "f1.f2" date not null,
    data text);


select * from "s1.s2".info('"s3.s4"."t1.t2"', '2014-09-15');
select "s1.s2".create_for('"s3.s4"."t1.t2"', '2014-09-15');
select "s1.s2".setup('"s3.s4"."t1.t2"', 'f1.f2', 'monthly',
    '{{nmonths,3}}');
select * from "s1.s2".info('"s3.s4"."t1.t2"', '2014-09-15');

insert into "s3.s4"."t1.t2" values (100, '2014-09-15', 'first');
select "s1.s2".create_for('"s3.s4"."t1.t2"', '2014-09-15');
select * from "s1.s2".info('"s3.s4"."t1.t2"', '2014-09-15');
insert into "s3.s4"."t1.t2" values (100, '2014-09-15', 'first');
select * from only "s3.s4"."t1.t2";
select * from only "s3.s4"."t1.t2_201407";

-- Update, no partition change
update "s3.s4"."t1.t2" set "f1.f2" = '2014-8-15' where id = 100;
select * from only "s3.s4"."t1.t2";
select * from only "s3.s4"."t1.t2_201407";

-- Update to fail partition
update "s3.s4"."t1.t2" set "f1.f2" = '2014-10-15' where id = 100;
select * from only "s3.s4"."t1.t2";
select * from only "s3.s4"."t1.t2_201407";

-- Create the missing partition and try again
select "s1.s2".create_for('"s3.s4"."t1.t2"', '2014-10-15');
update "s3.s4"."t1.t2" set "f1.f2" = '2014-10-15' where id = 100;
select * from only "s3.s4"."t1.t2";
select * from only "s3.s4"."t1.t2_201407";
select * from only "s3.s4"."t1.t2_201410";

-- Detach the partition removes it from writing
insert into "s3.s4"."t1.t2" values (104, '2014-07-10', 'third');
select "s1.s2".detach_for('"s3.s4"."t1.t2"', '2014-07-10');
-- Partition is there but removed from the base table
select * from "s3.s4"."t1.t2" where "f1.f2" = '2014-07-10';
-- Trigger has been maintained
insert into "s3.s4"."t1.t2" values (105, '2014-07-10', 'fourth');
insert into "s3.s4"."t1.t2" values (106, '2014-10-10', 'fifth');
-- Can't create the same partition again
select "s1.s2".create_for('"s3.s4"."t1.t2"', '2014-07-10');
-- But can attach it back
select "s1.s2".attach_for('"s3.s4"."t1.t2"', '2014-07-10');
insert into "s3.s4"."t1.t2" values (105, '2014-07-10', 'fourth');
select * from "s3.s4"."t1.t2" where "f1.f2" = '2014-07-10' order by id;

-- No shadow with tables in other schemas
create schema "sha.dow";
set search_path to "sha.dow", "$user", public;
create table "sha.dow"."sha.tbl" (
    id serial primary key,
    day date not null,
    data text);
select "s1.s2".setup('"sha.tbl"', 'day', 'monthly');
select "s1.s2".create_for('"sha.tbl"', '2015-01-15');

create temp table "sha.tbl" (like "sha.dow"."sha.tbl");

insert into "sha.dow"."sha.tbl" values (1, '2015-01-15', 'shadow1');
insert into "sha.dow"."sha.tbl" values (2, '2015-02-15', 'shadow2');
select "s1.s2".create_for('"sha.dow"."sha.tbl"', '2015-02-15');
insert into "sha.dow"."sha.tbl" values (2, '2015-02-15', 'shadow2');

drop table "sha.tbl";
select tableoid::regclass, * from "sha.tbl" order by id;

reset search_path;


-- Constraints, indexes, triggers, options
create table "s3.s4"."constr.1" (id1 int, id2 int, primary key (id1, id2));
insert into "s3.s4"."constr.1" values (1,2), (3,4);
create table "s3.s4"."constr.2" (
    id serial primary key,
    date date not null,
    fid1 int, fid2 int,
    foreign key (fid1, fid2) references "s3.s4"."constr.1" (id1, id2),
    uint int check (uint > 0),
    unique (uint),
    iint int,
    c circle,
    exclude using gist (c with &&)
) with (autovacuum_enabled = true, fillfactor = 23);

create index constr2_iint on "s3.s4"."constr.2" (iint) with (fillfactor = 69);
create unique index somename on "s3.s4"."constr.2" (iint) where id > 0;
create unique index taken on "s3.s4"."constr.2" (iint) where id > 1;
create table "s3.s4"."constr.2_201409_taken" ();

create function "tr.g_f"() returns trigger language plpgsql as $$
begin
	return new;
end
$$;

create trigger "tr.g1" before insert or update on "s3.s4"."constr.2" for each row execute procedure "tr.g_f"();
create trigger "tr.g2" after insert or update on "s3.s4"."constr.2" for each row when (new.fid1 > new.fid2) execute procedure "tr.g_f"();
create trigger "tr.g3" after insert or delete on "s3.s4"."constr.2" for each statement execute procedure "tr.g_f"();
create trigger "_tr.g4" after insert or delete on "s3.s4"."constr.2" for each statement execute procedure "tr.g_f"();
alter table "s3.s4"."constr.2" disable trigger "tr.g3";

select "s1.s2".setup('"s3.s4"."constr.2" ', 'date', 'monthly');
select "s1.s2".create_for('"s3.s4"."constr.2" ', '2014-09-01');

select conname, pg_get_constraintdef(oid, true) from pg_constraint
where conrelid = '"s3.s4"."constr.2_201409"'::regclass
order by conname;

select replace(pg_get_indexdef(indexrelid), '''', '') as idx from pg_index
where indrelid = '"s3.s4"."constr.2_201409"'::regclass
order by 1;

select unnest(reloptions) from pg_class
where oid = '"s3.s4"."constr.2"'::regclass
order by 1;

select tgname, tgenabled, pg_get_triggerdef(t.oid) from pg_trigger t
where tgrelid = '"s3.s4"."constr.2_201409"'::regclass and not tgisinternal
order by 1;


-- Table with nullable partition field
create table "s3.s4"."t1.t3" (
    id serial primary key,
    "f1.f2" date,
    data text);

select "s1.s2".setup('"s3.s4"."t1.t3"', 'f1.f2', 'monthly');

insert into "s3.s4"."t1.t3" (data) values ('hi');
select * from "s3.s4"."t1.t3";

-- Fails insert
insert into "s3.s4"."t1.t3" ("f1.f2", data) values ('2017-03-20', 'oh');
select "s1.s2".create_for('"s3.s4"."t1.t3"', '2017-03-01');

insert into "s3.s4"."t1.t3" ("f1.f2", data) values ('2017-03-20', 'oh');
select * from "s3.s4"."t1.t3" order by id;

-- Can go to null partition
update "s3.s4"."t1.t3" set "f1.f2" = null where data = 'oh';
select * from "s3.s4"."t1.t3" order by id;

-- Can go to non-null partition
update "s3.s4"."t1.t3" set "f1.f2" = '2017-03-21' where data = 'hi';
select * from "s3.s4"."t1.t3" order by id;

-- Can change partition
update "s3.s4"."t1.t3" set "f1.f2" = '2017-04-20' where data = 'hi';
select "s1.s2".create_for('"s3.s4"."t1.t3"', '2017-04-01');
update "s3.s4"."t1.t3" set "f1.f2" = '2017-04-20' where data = 'hi';
select * from "s3.s4"."t1.t3" order by id;
select * from only "s3.s4"."t1.t3" order by id;


-- Ownership
create user "u.1";
create user "u.2";
create schema "test.own";
grant create, usage on schema "test.own" to public;
set session authorization "u.1";
create table "test.own"."t.1" (id int primary key, date date);
grant insert on table "test.own"."t.1" to "u.2";
grant select on table "test.own"."t.1" to "u.2" with grant option;
reset session authorization;
create table "test.own"."t.2" (id int primary key, date date);
alter table "test.own"."t.2" owner to "u.2";

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

select "s1.s2".setup('"test.own"."t.1"', 'date', 'monthly');
select "s1.s2".create_for('"test.own"."t.1"', '2014-09-01');
select * from comp_acls('"test.own"."t.1"', '"test.own"."t.1_201409"');
select usename from pg_user u join pg_class c on c.relowner = u.usesysid
where c.oid = '"test.own"."t.1"'::regclass;

select "s1.s2".setup('"test.own"."t.2"', 'date', 'monthly');
select "s1.s2".create_for('"test.own"."t.2"', '2014-09-01');
select * from comp_acls('"test.own"."t.2"', '"test.own"."t.2_201409"');
select usename from pg_user u join pg_class c on c.relowner = u.usesysid
where c.oid = '"test.own"."t.2"'::regclass;

set client_min_messages to 'error';
drop schema "test.own" cascade;
reset client_min_messages;
drop user "u.1";
drop user "u.2";

drop extension pgparts;
