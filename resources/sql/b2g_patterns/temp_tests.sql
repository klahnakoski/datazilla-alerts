
use b2g_tests;

drop table if exists temp_test_runs;

create table temp_test_runs AS
  select distinct
    id test_run_id,
    revision, 
    branch,
    suite test_name,
    `date` date_received
  from 
    b2g_tests
;