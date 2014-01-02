##create database test;
use test;

CALL util.dir_list('C:/users/klahnakoski', 'test.temp');
select * from test.temp;
