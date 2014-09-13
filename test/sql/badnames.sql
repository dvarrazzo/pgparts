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
select "s1.s2".setup('"s3.s4"."t1.t2"'::regclass, 'f1.f2', 'monthly', '{3}');
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

drop extension pgparts;
