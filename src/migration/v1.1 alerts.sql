
DELIMITER ;;




CREATE TABLE IF NOT EXISTS util_uid_next(
	id 		BIGINT
);;

DROP FUNCTION IF EXISTS util_newID;;
CREATE FUNCTION util_newID () 
	RETURNS BIGINT
	READS SQL DATA
BEGIN
	IF @util_curr_id IS NULL THEN
		SELECT max(id) INTO @util_curr_id FROM util_uid_next;
		IF @util_curr_id IS NULL THEN 
			INSERT INTO util_uid_next VALUES (0);
			SET @util_curr_id=0;
		END IF;		
		UPDATE util_uid_next SET id=@util_curr_id+1000;
	ELSEIF @util_curr_id%1000=0 THEN
		SELECT max(id) INTO @util_curr_id FROM util_uid_next;
		UPDATE util_uid_next SET id=@util_curr_id+1000;
	END IF;
	
	SET @util_curr_id=@util_curr_id+1;
	RETURN @util_curr_id-1;
END;;



DROP PROCEDURE IF EXISTS exec;;
CREATE PROCEDURE exec(
	sqltext		VARCHAR(8000),
	ignore_error 	TINYINT
)
BEGIN
	IF ignore_error=1 THEN BEGIN
		DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;
		SET @temp=sqltext;
		PREPARE stmt FROM @temp;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	END; ELSE BEGIN
		SET @temp=sqltext;
		PREPARE stmt FROM @temp;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	END; END IF;
END;;



DROP PROCEDURE IF EXISTS get_version;;
CREATE PROCEDURE get_version(
	OUT version VARCHAR(10)
) BEGIN
	DECLARE is_version_1 INTEGER;
	DECLARE schema_name VARCHAR(80);
	
	SELECT DATABASE() INTO schema_name FROM DUAL;
	
	SELECT count(1) INTO is_version_1 FROM information_schema.tables WHERE table_schema=schema_name AND table_name='database';
	IF (is_version_1=0) THEN 
		CALL exec(concat('CREATE TABLE ', schema_name, '.database (version	VARCHAR(10))'), false);
		CALL exec(concat('INSERT INTO ', schema_name, '.database VALUES (''1.0'')'), false);
		SET version='1.0';
	ELSE 
		SET @version='1.0';
		CALL exec(concat('SELECT max(version) INTO @version FROM ', schema_name, '.database'), false);
		SET version=@version;
	END IF;	
END;;


DROP FUNCTION IF EXISTS bayesian_add;;
CREATE FUNCTION bayesian_add (
	a	DOUBLE,
	b	DOUBLE
) 
	RETURNS DOUBLE
 	DETERMINISTIC
	NO SQL
BEGIN
	RETURN a*b/(a*b+(1-a)*(1-b));
END;;



DROP PROCEDURE IF EXISTS `migrate v1.1`;;
CREATE PROCEDURE `migrate v1.1` ()
m11: BEGIN
	DECLARE version VARCHAR(10);
	
	CALL get_version(version);
	IF (version<>'1.0') THEN
		LEAVE m11;
	END IF;
	
	
	DROP TABLE IF EXISTS alerts;
	DROP TABLE IF EXISTS alert_reasons;
	DROP TABLE IF EXISTS alert_stati;
	DROP TABLE IF EXISTS alert_listeners;
	DROP TABLE IF EXISTS alert_page_thresholds;
	
	CREATE TABLE alert_stati(
		code			VARCHAR(10) NOT NULL PRIMARY KEY
	);
	INSERT INTO alert_stati VALUES ('new');
	INSERT INTO alert_stati VALUES ('obsolete');	##MAYBE THIS IS USEFUL


	CREATE TABLE alert_reasons (
		code			VARCHAR(80) NOT NULL PRIMARY KEY,
		description		VARCHAR(2000), ##MORE DETAILS ABOUT WHAT THIS IS
		last_run		DATETIME NOT NULL,
		config			VARCHAR(8000)
	);
	INSERT INTO alert_reasons VALUES (
		'page_threshold_limit', 
		concat('The page has performed badly (',char(36),'{actual}), ',char(36),'{expected} or less was expected'),
		date_add(now(), INTERVAL -30 DAY),
		null
	);		
	INSERT INTO alert_reasons VALUES (
		'alert_exception',
		concat(''',char(36),''{url} has performed worse then usual by ',char(36),'{stddev} standard deviations (',char(36),'{confidence})'),
		date_add(now(), INTERVAL -30 DAY),
		'{"minOffset":0.999}'
	);
	INSERT INTO alert_reasons VALUES (
		'alert_revision',
		concat(''',char(36),''{url} has performed worse then usual by ',char(36),'{stddev} standard deviations (',char(36),'{confidence})'),
		date_add(now(), INTERVAL -30 DAY),
		'{"minOffset":0.999}'
	);


	CREATE TABLE alert_page_thresholds (
		id				INTEGER NOT NULL PRIMARY KEY,
		page			INTEGER NOT NULL,
		threshold		DECIMAL(20, 10) NOT NULL,
		severity		DOUBLE NOT NULL, 
		reason			VARCHAR(2000) NOT NULL,
		time_added		DATETIME NOT NULL,
		contact			VARCHAR(200) NOT NULL,
		FOREIGN KEY (page) REFERENCES pages(id) 
	);
	
	INSERT INTO alert_page_thresholds
	SELECT
		util_newID(),
		p.id,
		200,
		0.5,
		"(mozilla.com) because I like to complain",
		now(),
		"klahnakoski@mozilla.com"
	FROM
		pages p 
	WHERE
		p.url='mozilla.com'
	;
	

	CREATE TABLE alert_listeners (
		email			VARCHAR(200) NOT NULL PRIMARY KEY
	);
	INSERT INTO alert_listeners VALUES ('klahnakoski@mozilla.com');


	#ALTER TABLE test_data_all_dimensions ADD UNIQUE INDEX tdad_id(id)
	
	CREATE TABLE alerts (
		id 				INTEGER NOT NULL PRIMARY KEY,
		status	 		VARCHAR(10) NOT NULL,  ##FOR ALERT LOGIC TO MARKUP, MAYBE AS obsolete
		create_time		DATETIME NOT NULL,		##WHEN THIS ISSUE WAS FIRST IDENTIFIED
		last_updated	DATETIME NOT NULL, 	##WHEN THIS ISSUE WAS LAST UPDATED WITH NEW INFO
		last_sent		DATETIME,			##WHEN THIS ISSUE WAS LAST SENT TO EMAIL
		tdad_id			INTEGER NOT NULL, 	##REFERENCE THE SMALLEST TESTING OBJECT (AT THIS TIME)
		reason			VARCHAR(20) NOT NULL,  ##REFERNCE TO STANDARD SET OF REASONS
		details			VARCHAR(2000) NOT NULL, ##JSON OF SPECIFIC DETAILS
		severity		DOUBLE NOT NULL,		##ABSTRACT SEVERITY 1.0==HIGH, 0.0==LOW
		confidence		DOUBLE NOT NULL,		##CONFIDENCE INTERVAL 1.0==100% CONFIDENCE
		solution		VARCHAR(40), ##INTENT FOR HUMANS TO MARKUP THIS ALERT SO MACHINE KNOWS IF REAL, OR START ESCALATING
		INDEX alert_lookup (tdad_id),
		#FOREIGN KEY alert_tdad_id (tdad_id) REFERENCES test_data_all_dimensions(id),
		FOREIGN KEY alert_status (status) REFERENCES alert_stati(code),
		FOREIGN KEY alert_reason (reason) REFERENCES alert_reasons(code)
	);
	
	UPDATE `database` SET version='1.1';
	
END;;


CALL `migrate v1.1`();;
COMMIT;;








