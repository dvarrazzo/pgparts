\pset format unaligned
\pset footer off
\set VERBOSITY terse
create schema irrtest;
set datestyle = 'ISO';

create extension pgparts with schema irrtest;

create table irr (
    id serial primary key,
    day date not null);

-- Must be partitioned
select irrtest.create_partition('irr', '2016-01-01', '2017-01-01');

-- Check overlapping
select irrtest.setup('irr', 'day', 'monthly');
select irrtest.create_partition('irr', '2016-01-01', '2017-01-01');
begin;
select irrtest.create_for('irr', '2015-12-10');
rollback;
select irrtest.create_for('irr', '2016-01-10');
select irrtest.create_for('irr', '2016-12-10');
begin;
select irrtest.create_for('irr', '2017-01-10');
rollback;

select irrtest.create_partition('irr', '2016-01-01', '2017-01-01');
select irrtest.create_partition('irr', '2015-01-01', '2018-01-01');
select irrtest.create_partition('irr', '2016-05-01', '2016-07-01');
select irrtest.create_partition('irr', '2015-01-01', '2016-01-02');
begin;
select irrtest.create_partition('irr', '2015-01-01', '2016-01-01');
rollback;
select irrtest.create_partition('irr', '2016-12-31', '2018-01-01');
begin;
select irrtest.create_partition('irr', '2017-01-01', '2018-01-01');
rollback;

-- Play with data
select irrtest.create_for('irr', '2015-12-10');
select irrtest.create_for('irr', '2017-01-10');

insert into irr (day) values
	('2015-12-31'), ('2016-01-01'), ('2016-12-31'), ('2017-01-01');

select tableoid::regclass, day from irr order by day;

-- test with timestamptz
create table irrtz (
    id serial primary key,
    ts timestamptz not null);

select irrtest.setup('irrtz', 'ts', 'monthly', '{{nmonths,3}}');

select irrtest.create_for('irrtz', '2015-12-10Z');
select irrtest.create_for('irrtz', '2017-01-10Z');
select irrtest.create_partition('irrtz',
	('2016-01-01Z'::timestamptz - '1 sec'::interval)::text, '2017-01-01Z');
select irrtest.create_partition('irrtz',
	'2016-01-01Z', ('2017-01-01Z'::timestamptz + '1 sec'::interval)::text);
select irrtest.create_partition('irrtz', '2016-01-01Z', '2017-01-01Z');

select table_name, start_value, end_value from irrtest.partition
where base_table = 'irrtz'::regclass order by start_value;

insert into irrtz (ts) values
	('2015-12-31T23:59:59Z'), ('2016-01-01Z'),
	('2016-12-31T23:59:59Z'), ('2017-01-01Z');

select tableoid::regclass, ts from irrtz order by ts;

-- Partition manipulation
select (irrtest.info('irr'::regclass, day::text)).* from irr order by day;

select irrtest.detach_for('irr', '2016-01-01');
select tableoid::regclass, day from irr order by day;

select irrtest.attach_for('irr', '2016-01-01');
select tableoid::regclass, day from irr order by day;

select irrtest.create_archive('irr');
select irrtest.archive_before('irr', '2016-12-31');
select irrtest.archive_before('irr', '2016-12-31');
select irrtest.archive_before('irr', '2017-01-01');
select irrtest.archive_before('irr', '2017-01-01');

select * from irr order by day;
select (irrtest.info('irr'::regclass, day::text)).* from irr_all order by day;

select irrtest.unarchive_partition('irr_201601_201701');

select * from irr order by day;
select (irrtest.info('irr'::regclass, day::text)).* from irr_all order by day;

drop extension pgparts;
