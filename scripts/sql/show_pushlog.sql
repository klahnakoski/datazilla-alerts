use pushlog_hgmozilla_1;



SELECT
	c.author,
	c.branch,
	c.`desc`,
	c.node,
	FROM_UNIXTIME(p.`date`) `date`,
	p.user,
	b.name,
	b.uri
FROM
	changesets c
JOIN
	pushlogs p on p.id = c.pushlog_id
JOIN
	branches b on b.id = p.branch_id
WHERE
	b.name='Mozilla-Inbound'
ORDER BY
	p.date DESC
LIMIT
	1000