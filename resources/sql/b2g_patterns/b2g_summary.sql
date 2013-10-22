use b2g_tests;


select
  concat('sum(CASE WHEN branch=''', branch, ''' THEN 1 ELSE 0 END) `', branch, '`,')
from
  b2g_tests
group by
  branch
;

SET time_zone='+0:00';

select
  arcavia_time.floor(FROM_UNIXTIME(`date`), 'DAY') day,
  sum(CASE WHEN branch='master' THEN 1 ELSE 0 END) `master`,
  sum(CASE WHEN branch='nightly' THEN 1 ELSE 0 END) `nightly`,
  sum(CASE WHEN branch='v1-train' THEN 1 ELSE 0 END) `v1-train`,
  sum(CASE WHEN branch='v1.0.0' THEN 1 ELSE 0 END) `v1.0.0`,
  sum(CASE WHEN branch='v1.0.1' THEN 1 ELSE 0 END) `v1.0.1`,
  sum(CASE WHEN branch='v1.2' THEN 1 ELSE 0 END) `v1.2`,
  count(1) num
from
  b2g_tests
group by
  arcavia_time.floor(FROM_UNIXTIME(`date`), 'DAY')
order by
  arcavia_time.floor(FROM_UNIXTIME(`date`), 'DAY')
;