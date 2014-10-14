use alerts;

SELECT
	a.*
FROM
	alerts a
WHERE
-- 	a.revision ='aed50d3edf33' and
	a.test = '1-customize-enter.error.TART' and 
--  	a.branch = 'Mozilla-Aurora' and
--   	a.reason like '%rev%' and
-- 	last_sent is null and
	push_date > date_add(now(), interval -30 day) #and
-- 	status <> 'obsolete'
ORDER BY
-- 	json.string(json.json(a.details, "Test"), "name"),
	a.push_date desc,
	a.revision
;
