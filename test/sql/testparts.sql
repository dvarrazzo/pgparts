create schema partest;
set datestyle = 'ISO';

create extension pgparts with schema partest;

create schema "n1.n2";

create table sometbl (
    id serial primary key,
    day date not null,
    data text);

create table "n1.n2"."t1.t2" (
    id serial primary key,
    day date not null,
    data text);

select * from partest.info('sometbl', '2014-09-15');

select partest.create_for('sometbl', '2014-09-15');

select partest.setup('sometbl'::regclass, 'day', 'monthly', '{3}');

select * from partest.info('sometbl', '2014-09-15');

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
