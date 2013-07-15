use ekyle_perftest_1;


SELECT 
-- 	c.desc,
	d.* 
from 
	test_data_all_dimensions d
-- left join
-- 	pushlog_hgmozilla_1.changesets c on c.revision=d.revision
WHERE
	test_name='tp5o' AND
--  	page_url='amazon.com' AND
 	page_url="yelp.com"
-- 	h0_rejected=1
-- 	operating_system_name='win' AND
-- 	operating_system_version='6.2.9200' AND
-- 	processor='x86_64'
-- 	n_replicates is null
ORDER BY
	test_name,
	branch,
	branch_version,
	operating_system_name,
	operating_system_version,
	processor,
	page_id,
    test_run_id,
    page_id,
    coalesce(push_date, date_received)
