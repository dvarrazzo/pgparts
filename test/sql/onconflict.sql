\pset format unaligned
\pset footer off
\set VERBOSITY terse
create schema pconf;
set datestyle = 'ISO';

create extension pgparts with schema pconf;


-- No drop on conflict

create table conftbl0 (
    id serial primary key,
    day date not null,
    data text unique);

select pconf.setup('conftbl0', 'day', 'monthly',
    '{{nmonths,3},{on_conflict_drop,false}}');
select pconf.create_for('conftbl0'::regclass, '2015-12-10');

insert into conftbl0 (id, day, data) values (10, '2015-12-10', 'foo');
insert into conftbl0 (id, day, data) values (10, '2015-12-11', 'bar');
insert into conftbl0 (id, day, data) values (11, '2015-12-12', 'foo');

select tableoid::regclass, * from conftbl0 order by id;


-- Drop on conflict on montly schema

create table conftbl1 (
    id serial primary key,
    day date not null,
    data text unique);

select pconf.setup('conftbl1', 'day', 'monthly',
    '{{nmonths,3},{on_conflict_drop,true}}');
select pconf.create_for('conftbl1'::regclass, '2015-12-10');

insert into conftbl1 (id, day, data) values (10, '2015-12-10', 'foo');
insert into conftbl1 (id, day, data) values (10, '2015-12-11', 'bar');
insert into conftbl1 (id, day, data) values (11, '2015-12-12', 'foo');

select tableoid::regclass, * from conftbl1 order by id;


-- Drop on conflict on daily schema

create table conftbl2 (
    id serial primary key,
    day date not null,
    data text unique);

select pconf.setup('conftbl2', 'day', 'daily',
    '{{ndays,7},{on_conflict_drop,true}}');
select pconf.create_for('conftbl2'::regclass, '2015-12-10');

insert into conftbl2 (id, day, data) values (10, '2015-12-10', 'foo');
insert into conftbl2 (id, day, data) values (10, '2015-12-11', 'bar');
insert into conftbl2 (id, day, data) values (11, '2015-12-12', 'foo');

select tableoid::regclass, * from conftbl2 order by id;


drop extension pgparts;
