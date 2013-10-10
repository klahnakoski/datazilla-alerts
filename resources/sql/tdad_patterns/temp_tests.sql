
use ekyle_perftest_1;

drop table if exists temp_test_runs;

create table temp_test_runs AS
  select distinct
    test_run_id,
    revision, 
    branch,
    branch_version,
    product,
    concat(operating_system_name, '|', operating_system_version, '|', processor) system,
    replace(test_name, "Talos ", "") test_name,
    date_received
  from 
    test_data_all_dimensions
  where
    unix_timestamp(str_to_date('150413', '%d%m%y'))<=date_received AND
    date_received<unix_timestamp(str_to_date('150613', '%d%m%y'))
;

select count(1), max(FROM_UNIXTIME(date_received))
  from 
    jeads_perftest_1.test_data_all_dimensions
  where
    date_received>unix_timestamp(date_add(now(), INTERVAL -120 DAY))
