
use pushlog_hgmozilla_1;


create table pushlogs_backup as
select * from pushlogs
;

create table changesets_backup as
select * from changesets
;

select count(1) from pushlogs;
select count(1) from pushlogs_backup;
select count(1) from changesets;
select count(1) from changesets_backup;


