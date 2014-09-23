use alerts;

SELECT
	a.*
FROM
	alerts a
WHERE
--  	a.reason like 'b2g%' and
	last_sent is null and
	push_date > date_add(now(), interval -30 day) and
	status <> 'obsolete'
ORDER BY
-- 	json.string(json.json(a.details, "Test"), "name"),
	a.push_date desc,
	a.revision
;
