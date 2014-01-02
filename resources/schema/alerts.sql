DROP DATABASE IF EXISTS alerts;
CREATE DATABASE alerts;
USE alerts;
DELIMITER ;;


DROP TABLE IF EXISTS alerts;;
DROP TABLE IF EXISTS reasons;;
DROP TABLE IF EXISTS stati;;
DROP TABLE IF EXISTS listeners;;
DROP TABLE IF EXISTS page_thresholds;;

CREATE TABLE stati (
	code VARCHAR(10) NOT NULL PRIMARY KEY
);;
INSERT INTO stati VALUES ('new');;
INSERT INTO stati VALUES ('obsolete');;


CREATE TABLE reasons (
	code           VARCHAR(80) NOT NULL PRIMARY KEY,
	description    VARCHAR(2000), ##MORE DETAILS ABOUT WHAT THIS IS
	last_run       DATETIME    NOT NULL,
	config         VARCHAR(8000),
	email_template VARCHAR(8000)
);;
INSERT INTO reasons VALUES (
	'page_threshold_limit',
	concat('The page has performed badly ({{actual}}), {{expected}} or less was expected'),
	date_add(now(), INTERVAL -30 DAY),
	NULL,
	NULL
);;
INSERT INTO reasons VALUES (
	'alert_exception',
	concat('{{page_url}} has performed worse then usual by {{diff}} standard deviations ({{confidence}})'),
	date_add(now(), INTERVAL -30 DAY),
	'{"minOffset":0.999}',
	NULL
);;
INSERT INTO reasons VALUES (
	'alert_revision',
	concat('{{page_url}} has performed worse then usual by {{diff}} standard deviations ({{confidence}})'),
	date_add(now(), INTERVAL -30 DAY),
	'{"minOffset":0.999}',
	NULL
);;
INSERT INTO reasons VALUES (
	'alert_sustained',
	concat('{{page_url}} has continued to perform worse since {{revision}}'),
	date_add(now(), INTERVAL -30 DAY),
	'{"minOffset":0.999}',
	NULL
);;


CREATE TABLE page_thresholds (
	id         INTEGER         NOT NULL PRIMARY KEY,
	page       INTEGER         NOT NULL,
	threshold  DECIMAL(20, 10) NOT NULL,
	severity   DOUBLE          NOT NULL,
	reason     VARCHAR(2000)   NOT NULL,
	time_added DATETIME        NOT NULL,
	contact    VARCHAR(200)    NOT NULL,
	FOREIGN KEY (page) REFERENCES pages (id)
);;

INSERT INTO page_thresholds
	SELECT
		util.newID(),
		p.id,
		200,
		0.5,
		"(mozilla.com) because I like to complain",
		now(),
		"klahnakoski@mozilla.com"
	FROM
		pages p
	WHERE
		p.url = 'mozilla.com'
;;


CREATE TABLE listeners (
	email VARCHAR(200) NOT NULL PRIMARY KEY
);;
INSERT INTO listeners VALUES ('klahnakoski@mozilla.com');;


#ALTER TABLE test_data_all_dimensions ADD UNIQUE INDEX tdad_id(id)

CREATE TABLE alerts (
	id           INTEGER     NOT NULL PRIMARY KEY,
	status       VARCHAR(10) NOT NULL, ##FOR ALERT LOGIC TO MARKUP, MAYBE AS obsolete
	create_time  DATETIME    NOT NULL, ##WHEN THIS ISSUE WAS FIRST IDENTIFIED
	last_updated DATETIME    NOT NULL, ##WHEN THIS ISSUE WAS LAST UPDATED WITH NEW INFO
	last_sent    DATETIME,             ##WHEN THIS ISSUE WAS LAST SENT TO EMAIL
	tdad_id      INTEGER     NOT NULL, ##REFERENCE THE SMALLEST TESTING OBJECT (AT THIS TIME)
	reason       VARCHAR(20) NOT NULL, ##REFERNCE TO STANDARD SET OF REASONS
	details      LONGTEXT,             ##JSON OF SPECIFIC DETAILS
	revision     VARCHAR(20),          ##FOR ALERTS THAT APPLY TO A REVISION
	severity     DOUBLE      NOT NULL, ##ABSTRACT SEVERITY 1.0==HIGH, 0.0==LOW
	confidence   DOUBLE      NOT NULL, ##CONFIDENCE INTERVAL 1.0==100% CONFIDENCE
	solution     VARCHAR(40), ##INTENT FOR HUMANS TO MARKUP THIS ALERT SO MACHINE KNOWS IF REAL, OR START ESCALATING
	INDEX alert_lookup (tdad_id),
	FOREIGN KEY alert_status (status) REFERENCES stati (code),
	FOREIGN KEY alert_reason (reason) REFERENCES reasons (code)
);;

CREATE UNIQUE INDEX alerts_reason_tdad_id ON alerts (tdad_id, reason);;

CREATE TABLE hierarchy (
	parent INTEGER,
	child  INTEGER,
	FOREIGN KEY (parent) REFERENCES alerts (id),
	FOREIGN KEY (child) REFERENCES alerts (id)
);;
