use ekyle_perftest_1;

SELECT
	json.string(details, "revision") revision,
	count(1) num
FROM
	alerts a
WHERE
	a.status<>'obsolete' and
	a.solution is not null and
	trim(a.solution) <> ''
GROUP BY
	json.string(details, "revision")
ORDER BY
	count(1) desc
LIMIT 
	20
;

SELECT
	from_unixtime(json.number(details, "push_date")) push_date,
	a.*
FROM
	alerts a
WHERE
	json.string(details, "revision") = '1426ffa9caf2'
	