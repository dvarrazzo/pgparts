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

drop extension pgparts;
