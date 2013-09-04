################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


DELIMITER ;;

USE ekyle_perftest_1;;

ALTER TABLE test_data_all_dimensions DROP FOREIGN KEY fk_test_run_id_tdad;;
ALTER TABLE test_data_all_dimensions MODIFY page_url varchar(255) NULL DEFAULT NULL;
ALTER TABLE test_data_all_dimensions MODIFY mean double NULL DEFAULT NULL;
ALTER TABLE test_data_all_dimensions MODIFY std double NULL DEFAULT NULL;


SELECT count(1) FROM test_data_all_dimensions;;
DELETE FROM test_data_all_dimensions;;


DROP FUNCTION IF EXISTS ekyle_perftest_1.string_between;;
CREATE FUNCTION ekyle_perftest_1.`string_between`(
	value		VARCHAR(60000) character set latin1,
	begin_tag	VARCHAR(40),
	end_tag		VARCHAR(40),
	start_index	INTEGER
) RETURNS varchar(60000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE e INTEGER;

	SET s=LOCATE(begin_tag, value, start_index);
	SET e=LOCATE(end_tag, value, s+length(begin_tag));

	IF s=0 OR e=0 THEN RETURN NULL; END IF;
	RETURN substring(value, s+length(begin_tag), e-s-length(begin_tag));
END;;
