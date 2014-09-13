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

-- Detach the partition removes it from writing
insert into sometbl values (104, '2014-07-10', 'third');
select partest.detach_for('sometbl', '2014-07-10');
-- Partition is there but removed from the base table
select * from sometbl where day = '2014-07-10';
select * from sometbl_201407 where day = '2014-07-10';
-- Trigger has been maintained
insert into sometbl values (105, '2014-07-16', 'fourth');
insert into sometbl values (106, '2014-10-16', 'fifth');
-- Idempotent
select partest.detach_for('sometbl', '2014-07-10');
-- Can't create the same partition again
select partest.create_for('sometbl', '2014-07-10');
-- But can attach it back
select partest.attach_for('sometbl', '2014-07-10');
select * from sometbl where day = '2014-07-10';
insert into sometbl values (105, '2014-07-16', 'fourth');
-- Idempotent
select partest.attach_for('sometbl', '2014-07-10');
