-- FOR MAKING A SMALLER VERSION OF THE DATABASE TO TEST AGAINST

use alerts;

DELETE FROM hierarchy WHERE parent IN (
	SELECT
		a.id
	FROM
		alerts a
	WHERE
		a.create_time<date_add(now(), interval -30 day)
);
DELETE FROM hierarchy WHERE child IN (
	SELECT
		a.id
	FROM
		alerts a
	WHERE
		a.create_time<date_add(now(), interval -30 day)
);
DELETE FROM alerts WHERE create_time<date_add(now(), interval -30 day);
COMMIT;

DELETE FROM listeners;
COMMIT;


