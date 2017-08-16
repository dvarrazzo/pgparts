\pset format unaligned
\pset footer off
\set VERBOSITY terse
create schema arctest;
set datestyle = 'ISO';

create extension pgparts with schema arctest;

create table at (
    id serial primary key,
    day date not null,
    data text);

-- Create some partitions and data
select arctest.setup('at', 'day', 'monthly');
select arctest.create_for('at', '2017-01-10');
select arctest.create_for('at', '2017-02-10');
select arctest.create_for('at', '2017-03-10');
insert into at (day) values ('2017-01-10');
insert into at (day) values ('2017-02-10');

-- Must create the archive before
select arctest.archive_before('at', '2017-02-15');

select arctest.create_archive('at');
select * from at order by id;
select * from at_all order by id;
select * from at_archived order by id;

-- Idempotent
select arctest.create_archive('at');
select * from at order by id;
select * from at_all order by id;
select * from at_archived order by id;

-- Archiving partitions
select arctest.archive_before('at', '2017-02-01');
select * from arctest.info('at', '2017-01-15');
select * from arctest.info('at', '2017-02-15');
select * from at order by id;
select * from at_all order by id;
select * from at_archived order by id;

-- Archived partition can't receive data
insert into at (day) values ('2017-01-10');
insert into at (day) values ('2017-02-10');
select * from at order by id;
select * from at_all order by id;
select * from at_archived order by id;

-- Can't re-create an archived partition
select arctest.create_for('at', '2017-01-10');

-- Archived partition can be unarchived
select arctest.unarchive_partition('at_201701');
select arctest.unarchive_partition('at_201702');
select * from arctest.info('at', '2017-01-15');
select * from arctest.info('at', '2017-02-15');

-- Unarchived partition can receive data
insert into at (day) values ('2017-01-10');
insert into at (day) values ('2017-02-10');
select * from at order by id;
select * from at_all order by id;
select * from at_archived order by id;


-- Archived partitions don't mess with the dropold option

create table arcdroptbl (
    id serial primary key,
    day date not null,
    data text);

select arctest.setup('arcdroptbl', 'day', 'monthly', '{{drop_old,true}}');

select arctest.create_for('arcdroptbl', '2017-01-10');
select arctest.create_for('arcdroptbl', '2017-02-10');
insert into arcdroptbl (day) values ('2017-01-10');
insert into arcdroptbl (day) values ('2017-02-10');

select arctest.create_archive('arcdroptbl');
select arctest.archive_before('arcdroptbl', '2017-02-01');

insert into arcdroptbl (day) values ('2017-01-11');     -- discarded
insert into arcdroptbl (day) values ('2017-02-11');

select tableoid::regclass, * from arcdroptbl_all order by id;


drop extension pgparts;
