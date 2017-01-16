drop database if exists testing;
create database testing;

create table dimensionA (
  id integer primary key,
  sub_value1 varchar2(20),
  sub_value2 varchar2(20),
);

create table facts (
  id integer primary key