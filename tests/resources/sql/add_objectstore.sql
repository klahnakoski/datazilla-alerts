use ekyle_objectstore_1;

DROP TABLE IF EXISTS objectstore2;
rename table objectstore to objectstore2;
alter table objectstore2 drop index id;
alter table objectstore2 drop index test_run_id;
alter table objectstore2 drop index objectstore_revision_branch;
alter table objectstore2 drop index objectstore_test_run_id;
alter table objectstore2 drop index objectstore_processed_flag;
alter table objectstore2 drop index objectstore_revision_branch2;
alter table objectstore2 drop index objectstore_processed_flag_plus;



-- DROP TABLE IF EXISTS objectstore;
CREATE TABLE objectstore (
	id                  INTEGER PRIMARY KEY NOT NULL,
	test_run_id         INTEGER UNIQUE KEY  NULL,
	date_loaded         INT(11)             NOT NULL,
	processed_cube      ENUM('ready', 'done') DEFAULT 'ready',
	processed_exception ENUM('ready', 'done') DEFAULT 'ready',
	processed_sustained ENUM('ready', 'done') DEFAULT 'ready',
	processed_sustained_median ENUM('ready', 'done') DEFAULT 'ready',
	revision            VARCHAR(12),
	branch              VARCHAR(40),
	json_blob           MEDIUMBLOB
)
	DEFAULT CHARSET =utf8;

INSERT INTO objectstore (id, test_run_id, date_loaded, revision, branch, json_blob)
SELECT
  id,
  test_run_id,
  date_loaded,
  revision,
  branch,
  json_blob
FROM
  objectstore2
;


CREATE INDEX objectstore_test_run_id ON objectstore(test_run_id);
CREATE INDEX objectstore_revision_branch ON objectstore(revision, branch);
CREATE INDEX objectstore_date_loaded ON objectstore(date_loaded);
CREATE INDEX objectstore_processed_cube on objectstore(processed_cube);
CREATE INDEX objectstore_processed_exception on objectstore(processed_exception);
CREATE INDEX objectstore_processed_sustained on objectstore(processed_sustained);
CREATE INDEX objectstore_processed_sustained_median on objectstore(processed_sustained_median);


UPDATE objectstore o
JOIN ekyle_perftest_1.test_data_all_dimensions t ON t.test_run_id = o.test_run_id
SET o.processed_cube='done';
