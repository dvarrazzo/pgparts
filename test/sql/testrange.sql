\pset format unaligned
\pset footer off
\set VERBOSITY terse
create schema prange;
set datestyle = 'ISO';

create extension pgparts with schema prange;

create table rangetbl (
    id serial primary key,
    range tstzrange not null,
    data text);

select prange.setup('rangetbl', 'range', 'monthly');

-- This insert fails
insert into rangetbl values (100, '[2014-09-15Z,2014-09-16Z)', 'first');
select * from only rangetbl;

select prange.create_for('rangetbl', '2014-09-15');
select * from prange.info('rangetbl', '2014-09-15');

-- Insert
insert into rangetbl values (100, '[2014-09-15Z,2014-09-16Z)', 'first');
select * from only rangetbl;
select * from only rangetbl_201409;

-- Split
insert into rangetbl values (101, '[2014-09-20Z,2014-10-10Z)', 'second');
select prange.create_for('rangetbl', '2014-10-10');
insert into rangetbl values (101, '[2014-09-20Z,2014-10-10Z)', 'second');
select * from only rangetbl_201409;
select * from only rangetbl_201410;

drop extension pgparts;
