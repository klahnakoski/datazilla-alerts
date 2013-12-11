USE ekyle_perftest_1;

DROP TABLE IF EXISTS ekyle_perftest_1.temp_results;

# FIND TERM
CREATE TABLE ekyle_perftest_1.temp_results AS
SELECT 
	t.test_run_id,
	t.push_date,
	t.mean,
	t.`product`,
	t.`operating_system_name`,
	t.`operating_system_version`,
	t.`branch`,
	t.`test_name`,
	t.`branch_version`,
	t.`processor`,
	t.`page_url`,
	t.revision
FROM 
	ekyle_perftest_1.test_data_all_dimensions p
LEFT JOIN
	ekyle_perftest_1.test_data_all_dimensions t 
ON
	t.`product`=p.`product` AND
	t.`operating_system_name`=p.`operating_system_name` AND
	t.`operating_system_version`=p.`operating_system_version` AND
	t.`branch`=p.`branch` AND
	t.`test_name`=p.`test_name` AND
	t.`branch_version`=p.`branch_version` AND
	t.`processor`=p.`processor` AND
	t.`page_url`=p.`page_url`
WHERE
	p.test_run_id=2631066  #id=23794811
	and p.page_url='imgur.com'
ORDER BY
	t.push_date DESC
;
-- 	https://datazilla.mozilla.org/?start=1383306067&stop=1383910867&product=Firefox&repository=Mozilla-Inbound-Non-PGO&os=win&os_version=6.2.9200&test=dromaeo_dom&graph_search=c9eb4218558d&tr_id=3460240&graph=modify.html&x86=false&error_bars=false&project=talos


# PULL DATA
SELECT 
	from_unixtime(t.push_date) push_date,
	t.mean median,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 0) `1`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 1) `2`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 2) `3`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 3) `4`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 4) `5`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 5) `6`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 6) `7`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 7) `8`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 8) `9`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 9) `10`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 10) `11`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 11) `12`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 12) `13`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 13) `14`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 14) `15`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 15) `16`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 16) `17`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 17) `18`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 18) `19`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 19) `20`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 20) `21`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 21) `22`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 22) `23`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 23) `24`,
	string_get_word(string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1), ",", 24) `25`,
	string_between(ekyle_perftest_1.json_a(json_blob, t.page_url), '[', ']', 1) raw_data,
	t.*
FROM
	ekyle_objectstore_1.objectstore o
JOIN
	ekyle_perftest_1.temp_results t ON t.test_run_id=o.test_run_id
WHERE
	o.test_run_id in (SELECT test_run_id FROM ekyle_perftest_1.temp_results)
-- 	and t.revision='c9eb4218558d'
ORDER BY
	t.test_name,
	t.page_url,
	t.push_date

