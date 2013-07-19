################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## Author: Kyle Lahnakoski (kyle@lahnakoski.com)
################################################################################




ALTER TABLE test_data_all_dimensions DROP FOREIGN KEY fk_test_run_id_tdad;;
ALTER TABLE test_data_all_dimensions MODIFY page_url varchar(255) NULL DEFAULT NULL;
ALTER TABLE test_data_all_dimensions MODIFY mean double NULL DEFAULT NULL;
ALTER TABLE test_data_all_dimensions MODIFY std double NULL DEFAULT NULL;


SELECT count(1) FROM test_data_all_dimensions;;
DELETE FROM test_data_all_dimensions;;

