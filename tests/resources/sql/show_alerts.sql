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
	a.details like '%5c90a723bb82%'
ORDER BY
	create_time DESC
;