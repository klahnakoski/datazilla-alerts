use alerts;

SELECT
--  	json.string(a.details, "Device") device,
--  	json.string(a.details, "Branch") branch,
 	json.string(json.json(a.details, "Test"), "name") test_name,
  	json.string(json.json(a.details, "Test"), "suite") test_suite,
	json.number(a.details, "diff_percent") diff_percent,	
	a.*	
FROM 
	alerts a 
WHERE
 	a.reason='talos_alert_revision' and
	last_sent is null and
	create_time > date_add(now(), interval -30 day) and
	status <> 'obsolete'
ORDER BY
-- 	json.string(json.json(a.details, "Test"), "name"),
	a.create_time desc,
	a.revision
;