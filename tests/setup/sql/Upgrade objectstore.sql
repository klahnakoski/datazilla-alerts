################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


DELIMITER ;;
use ekyle_objectstore_1;;



DROP TABLE IF EXISTS objectstore2;;
CREATE TABLE objectstore2 (
	id INTEGER UNIQUE KEY NOT NULL,
	test_run_id INTEGER PRIMARY KEY NOT NULL,
	date_loaded int(11) NOT NULL,
	revision VARCHAR(12),
	branch VARCHAR(40),
	json_blob mediumblob,
	KEY objectstore_revision_branch (revision, branch)
) DEFAULT CHARSET=utf8;;


INSERT INTO objectstore2 (
	id,
	test_run_id,
	date_loaded,
	revision,
	branch,
	json_blob
) SELECT
	id,
	test_run_id,
	date_loaded,
	ekyle_perftest_1.string_between(substring(json_blob, locate("revision\": \"", json_blob), 40), "revision\": \"", "\",", 1),
	ekyle_perftest_1.string_between(substring(json_blob, locate("branch\": \"", json_blob), 40), "branch\": \"", "\",", 1),
	json_blob
FROM
	objectstore
;;


DROP TABLE IF EXISTS objectstore_backup2;;
RENAME TABLE objectstore TO objectstore_backup2;;
RENAME TABLE objectstore2 TO objectstore;;



