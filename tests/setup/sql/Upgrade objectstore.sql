################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################


DELIMITER ;;
use ekyle_objectstore_1;;

ALTER TABLE objectstore DISABLE KEYS;
ALTER TABLE objectstore ADD COLUMN revision VARCHAR(12);;
ALTER TABLE objectstore ADD COLUMN branch VARCHAR(40);;
ALTER TABLE objectstore MODIFY page_url varchar(255) NULL DEFAULT NULL;;
ALTER TABLE objectstore MODIFY mean double NULL DEFAULT NULL;;
ALTER TABLE objectstore MODIFY std double NULL DEFAULT NULL;;
ALTER TABLE objectstore ENABLE KEYS;


#THE OBJECTSTORE WILL DICTATE THE test_run_id
UPDATE objectstore
SET test_run_id=ekyle_perftest_1.util_newid()
WHERE test_run_id IS NULL
;;

UPDATE objectstore 
SET revision=ekyle_perftest_1.string_between(substring(json_blob, locate("revision\": \"", json_blob), 40), "revision\": \"", "\",", 1)
WHERE revision IS NULL
;;

UPDATE objectstore
SET branch=ekyle_perftest_1.string_between(substring(json_blob, locate("branch\": \"", json_blob), 40), "branch\": \"", "\",", 1)
WHERE branch IS NULL
;;


CREATE INDEX objectstore_revision_branch ON objectstore(revision, branch);;



