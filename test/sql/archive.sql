\pset format unaligned
\pset footer off
\set VERBOSITY terse
create schema partest;
set datestyle = 'ISO';

create extension pgparts with schema partest;

create table at (
    id serial primary key,
    day date not null,
    data text);

select partest.setup('at', 'day', 'monthly');

select partest.create_archive('at');
select count(*) from at_all;
select count(*) from at_archived;

select partest.create_archive('at');
select count(*) from at_all;
select count(*) from at_archived;
