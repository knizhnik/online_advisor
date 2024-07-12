create extension online_advisor;

create table t(c1 integer, c2 integer, c3 integer);
insert into t values (generate_series(1,10000), generate_series(1,10000), generate_series(1,10000));

-- force loading of extension
select * from proposed_indexes;

select count(*) from t where c1 between 1000 and 2000;
select count(*) from t where c1=100 and c2=100;
select count(*) from t where c1=1 and c3=1;
-- check how column sets are grouped
select n_filtered,n_called,create_index from proposed_indexes;

-- check nested queries
create table t2(x integer, y integer);
insert into t2 values (generate_series(1,10000),0);
select * from t where c1 in (select y from t2 where x = 1);
select n_filtered,n_called,create_index from proposed_indexes;

-- check work with multiple tables
select * from t,t2 where c3=100 and y=200;
select n_filtered,n_called,create_index from proposed_indexes;

-- check work with multiple databases
create database somedb;
\c somedb
create extension online_advisor;
create table t3(x integer, y integer);
insert into t3 values (generate_series(1,10000),0);
select * from t3 where x < 0;
select n_filtered,n_called,create_index from proposed_indexes;



