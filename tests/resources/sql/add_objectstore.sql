
DROP TABLE IF EXISTS objectstore2;
DROP TABLE IF EXISTS objectstore;
CREATE TABLE objectstore (
	id INTEGER UNIQUE KEY NOT NULL,
	test_run_id INTEGER UNIQUE KEY NULL,
	date_loaded int(11) NOT NULL,
  processed_flag enum('ready','loading','complete', 'summary_ready', 'summary_loading', 'summary_complete') DEFAULT 'ready',
	revision VARCHAR(12),
	branch VARCHAR(40),
	json_blob mediumblob
) DEFAULT CHARSET=utf8;

CREATE INDEX objectstore_test_run_id ON objectstore(test_run_id);
CREATE INDEX objectstore_processed_flag_plus ON objectstore(processed_flag, revision, branch, test_run_id);
CREATE INDEX objectstore_revision_branch ON objectstore(revision, branch);
CREATE INDEX objectstore_date_loaded ON objectstore(date_loaded);
CREATE INDEX objectstore_processed_flag on objectstore(processed_flag);



