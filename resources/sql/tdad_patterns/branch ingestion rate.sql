use ekyle_perftest_1;

SELECT count(1) FROM temp_test_runs;

#TIME RANGE OF TESTS FOR EVERY REVISION
SELECT
  revision,
  branch, 
  branch_version,
  product,
  system,
  count(1),
  (max(date_received)-min(date_received))/60/60 diff_hours,
  group_concat(date_received SEPARATOR ',') `times`
FROM
  temp_test_runs 
GROUP BY 
  branch,
  branch_version,
  product,
  revision
ORDER BY 
  branch, 
  branch_version,
  product,
  revision
;


#POPULAR TEST COUNTS
SELECT
  product,
  num number_of_tests_per_revision,
  count(1) num_observed
FROM (
  SELECT
    revision,
    branch, 
    branch_version,
    product,
    count(1) num
  FROM
    temp_test_runs 
  GROUP BY 
    branch,
    branch_version,
    product,
    revision
  ) a
GROUP BY
  product,
  num
ORDER BY 
  count(1) DESC
;






#TIME BETWEEN TEST RESULTS
SET @last_revision=0;

SELECT 
  min_since_last,
  count(1) num
FROM (
  SELECT
      test_run_id,
      revision, 
      branch,
      branch_version,
      product,
      test_name,
      date_received,
      cast(CASE WHEN @last_revision=revision THEN (date_received-@last_time)/60 ELSE null END as DECIMAL) minutes_since_last,
      @last_time:=date_received last_time,
      @last_revision:=revision,
      from_unixtime(date_received)
  FROM (
    SELECT
      test_run_id,
      revision, 
      branch,
      branch_version,
      product,
      test_name,
      date_received
    FROM 
      temp_test_runs 
    WHERE 
      branch='Birch'
    ORDER BY
      branch, 
      branch_version,
      product,
      revision, 
      date_received,
      test_run_id
    ) a
) a
GROUP BY
  min_since_last
;


#TIME BETWEEN PLATFORM TEST RESULTS
SET @last_revision='0';

DROP TABLE IF EXISTS temp_tdad_times;
CREATE TABLE temp_tdad_times AS
  SELECT
      revision, 
      branch,
      branch_version,
      product,
      system,
      date_received,
      cast(CASE WHEN @last_revision=revision THEN (date_received-@first_time)/60 ELSE 0 END as DECIMAL) minutes_since_first,
      CASE WHEN @last_revision<>revision THEN @first_time:=date_received ELSE @first_time END first_time,
      cast(CASE WHEN @last_revision=revision THEN (date_received-@last_time)/60 ELSE null END as DECIMAL) minutes_since_last,
      @last_time:=date_received last_time,
      @last_revision:=revision,
      from_unixtime(date_received)
  FROM (
    SELECT
      cast(revision as char) revision, 
      branch,
      branch_version,
      product,
      system,
      cast(min(date_received) as decimal) date_received
    FROM 
      temp_test_runs 
--     WHERE 
--       branch='Birch'
    GROUP BY
      cast(revision as char), 
      branch,
      branch_version,
      product,
      system
    ORDER BY
      cast(revision as char), 
      min(date_received)
    ) a
;



CALL arcavia_util.integer_range("ekyle_perftest_1.temp_duration", "0, 1, 5, 10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 10000");
SELECT
  concat("  sum(CASE WHEN ",cast(`min` as char),"<=minutes_since_first AND minutes_since_first<",cast(`max` as char)," THEN 1 ELSE null END) `",`min`,"+`,") name
FROM
  temp_duration
;

SELECT 
  system,
  sum(CASE WHEN 0<=minutes_since_first AND minutes_since_first<1 THEN 1 ELSE null END) `0+`,
  sum(CASE WHEN 1<=minutes_since_first AND minutes_since_first<5 THEN 1 ELSE null END) `1+`,
  sum(CASE WHEN 5<=minutes_since_first AND minutes_since_first<10 THEN 1 ELSE null END) `5+`,
  sum(CASE WHEN 10<=minutes_since_first AND minutes_since_first<30 THEN 1 ELSE null END) `10+`,
  sum(CASE WHEN 30<=minutes_since_first AND minutes_since_first<60 THEN 1 ELSE null END) `30+`,
  sum(CASE WHEN 60<=minutes_since_first AND minutes_since_first<120 THEN 1 ELSE null END) `60+`,
  sum(CASE WHEN 120<=minutes_since_first AND minutes_since_first<180 THEN 1 ELSE null END) `120+`,
  sum(CASE WHEN 180<=minutes_since_first AND minutes_since_first<240 THEN 1 ELSE null END) `180+`,
  sum(CASE WHEN 240<=minutes_since_first AND minutes_since_first<300 THEN 1 ELSE null END) `240+`,
  sum(CASE WHEN 300<=minutes_since_first AND minutes_since_first<360 THEN 1 ELSE null END) `300+`,
  sum(CASE WHEN 360<=minutes_since_first AND minutes_since_first<420 THEN 1 ELSE null END) `360+`,
  sum(CASE WHEN 420<=minutes_since_first AND minutes_since_first<480 THEN 1 ELSE null END) `420+`,
  sum(CASE WHEN 480<=minutes_since_first AND minutes_since_first<540 THEN 1 ELSE null END) `480+`,
  sum(CASE WHEN 540<=minutes_since_first AND minutes_since_first<10000 THEN 1 ELSE null END) `540+`
FROM 
  temp_tdad_times a
GROUP BY
  system
ORDER BY
  system
;







CALL arcavia_time.`range`(
  'ekyle_perftest_1.tdad_daily',
  str_to_date('150413', '%d%m%y'), 
  str_to_date('150613', '%d%m%y'), 
  '1 DAY'
);


#BUILD THE SQL SELECT STATEMENT
drop table if exists tdad_branches;
create table tdad_branches as
select distinct
  branch,
  product
from
  temp_test_runs 
;  
select concat('  sum(case when branch=''',branch,''' and product=''', product, ''' THEN 1 ELSE 0 END) `', branch, '|', product, '`,') 
from tdad_branches
order by branch, product
;



select 
  test_name,
  count(1)
from
  temp_test_runs 
group by
  test_name
;  





SELECT
  d.name,
  sum(case when branch='Ash' and product='Fennec' THEN 1 ELSE 0 END) `Ash|Fennec`,
  sum(case when branch='Ash' and product='Firefox' THEN 1 ELSE 0 END) `Ash|Firefox`,
  sum(case when branch='Birch' and product='Firefox' THEN 1 ELSE 0 END) `Birch|Firefox`,
  sum(case when branch='Build-System' and product='Fennec' THEN 1 ELSE 0 END) `Build-System|Fennec`,
  sum(case when branch='Build-System' and product='Firefox' THEN 1 ELSE 0 END) `Build-System|Firefox`,
  sum(case when branch='Cedar' and product='Fennec' THEN 1 ELSE 0 END) `Cedar|Fennec`,
  sum(case when branch='Cedar' and product='Firefox' THEN 1 ELSE 0 END) `Cedar|Firefox`,
  sum(case when branch='Firefox' and product='Firefox' THEN 1 ELSE 0 END) `Firefox|Firefox`,
  sum(case when branch='Firefox-Non-PGO' and product='Firefox' THEN 1 ELSE 0 END) `Firefox-Non-PGO|Firefox`,
  sum(case when branch='Fx-Team' and product='Fennec' THEN 1 ELSE 0 END) `Fx-Team|Fennec`,
  sum(case when branch='Fx-Team' and product='Firefox' THEN 1 ELSE 0 END) `Fx-Team|Firefox`,
  sum(case when branch='Fx-Team-Non-PGO' and product='Firefox' THEN 1 ELSE 0 END) `Fx-Team-Non-PGO|Firefox`,
  sum(case when branch='Ionmonkey' and product='Fennec' THEN 1 ELSE 0 END) `Ionmonkey|Fennec`,
  sum(case when branch='Ionmonkey' and product='Firefox' THEN 1 ELSE 0 END) `Ionmonkey|Firefox`,
  sum(case when branch='Ionmonkey-Non-PGO' and product='Firefox' THEN 1 ELSE 0 END) `Ionmonkey-Non-PGO|Firefox`,
  sum(case when branch='Larch' and product='Fennec' THEN 1 ELSE 0 END) `Larch|Fennec`,
  sum(case when branch='Mozilla-Aurora' and product='Fennec' THEN 1 ELSE 0 END) `Mozilla-Aurora|Fennec`,
  sum(case when branch='Mozilla-Aurora' and product='Firefox' THEN 1 ELSE 0 END) `Mozilla-Aurora|Firefox`,
  sum(case when branch='Mozilla-Beta' and product='Fennec' THEN 1 ELSE 0 END) `Mozilla-Beta|Fennec`,
  sum(case when branch='Mozilla-Beta' and product='Firefox' THEN 1 ELSE 0 END) `Mozilla-Beta|Firefox`,
  sum(case when branch='Mozilla-Inbound' and product='Fennec' THEN 1 ELSE 0 END) `Mozilla-Inbound|Fennec`,
  sum(case when branch='Mozilla-Inbound' and product='Firefox' THEN 1 ELSE 0 END) `Mozilla-Inbound|Firefox`,
  sum(case when branch='Mozilla-Inbound-Non-PGO' and product='Firefox' THEN 1 ELSE 0 END) `Mozilla-Inbound-Non-PGO|Firefox`,
  sum(case when branch='Mozilla-Release' and product='Fennec' THEN 1 ELSE 0 END) `Mozilla-Release|Fennec`,
  sum(case when branch='Mozilla-Release' and product='Firefox' THEN 1 ELSE 0 END) `Mozilla-Release|Firefox`,
  sum(case when branch='Profiling' and product='Fennec' THEN 1 ELSE 0 END) `Profiling|Fennec`,
  sum(case when branch='Profiling' and product='Firefox' THEN 1 ELSE 0 END) `Profiling|Firefox`,
  sum(case when branch='Profiling-Non-PGO' and product='Firefox' THEN 1 ELSE 0 END) `Profiling-Non-PGO|Firefox`,
  sum(case when branch='Services-Central' and product='Fennec' THEN 1 ELSE 0 END) `Services-Central|Fennec`,
  sum(case when branch='Services-Central' and product='Firefox' THEN 1 ELSE 0 END) `Services-Central|Firefox`,
  sum(case when branch='Services-Central-Non-PGO' and product='Firefox' THEN 1 ELSE 0 END) `Services-Central-Non-PGO|Firefox`,
  sum(case when branch='Try' and product='Fennec' THEN 1 ELSE 0 END) `Try|Fennec`,
  sum(case when branch='Try' and product='Firefox' THEN 1 ELSE 0 END) `Try|Firefox`,
  sum(case when branch='Try-Non-PGO' and product='Firefox' THEN 1 ELSE 0 END) `Try-Non-PGO|Firefox`,
  sum(case when branch='UX' and product='Firefox' THEN 1 ELSE 0 END) `UX|Firefox`
FROM
  tdad_daily d
LEFT JOIN
  temp_test_runs r on d.min<=from_unixtime(r.date_received) and from_unixtime(r.date_received)<d.max
GROUP BY
  d.name,  
  d.value  
ORDER BY
  d.value
;  



