use alerts;

SELECT
	json.string(a.details, "Device") device,
	json.string(a.details, "Branch") branch,
	json.string(json.json(a.details, "Test"), "name") test_name,
	json.string(json.json(a.details, "Test"), "suite") test_suite,	
	a.*	
FROM 
	alerts a 
WHERE
	a.reason like 'talos%' and
	details like '%71a1dcb2697d%'
ORDER BY
	create_time DESC
LIMIT 
	100
;