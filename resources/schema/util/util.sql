drop database if exists util;
create database util;
use util;
SET SESSION binlog_format = 'ROW';

DELIMITER ;;

###############################################################################
## PREPARED STATEMENTS AND PROCEDURES HAVE LIMITS STARTING IN 5.1
##   * NO PARAMETERIZED "LOAD DATA INFILE" STATEMENTS ARE ALLOWED
##   * NO "LOAD DATA INFILE" ALLOWED IN PROCEDURES
## MAY ORACLE (THE CORPORATION) DIE A FIERY DEATH
##
## IT IS EXPECTED AN OUTSIDE DAEMON WILL EXECUTE THESE REQUESTED COMMANDS
###############################################################################
DROP TABLE IF EXISTS util.exec_queue;;
CREATE TABLE util.exec_queue(
	id 				BIGINT,
	request_time 	DATETIME,
	sqltext			VARCHAR(8000),
	ignore_error	TINYINT,
	response_time	DATETIME,
	response		VARCHAR(1000)
);;




DROP PROCEDURE IF EXISTS util.exec;;
CREATE PROCEDURE util.exec(
	sqltext		VARCHAR(8000),
	ignore_error 	TINYINT
)
BEGIN
	CALL debug.log(sqltext);
	IF
		sqltext REGEXP '.*LOAD[[:space:]]+DATA[[:space:]]+LOCAL[[:space:]]+INFILE[[:space:]]+' OR
		sqltext REGEXP '.*LOAD[[:space:]]+DATA[[:space:]]+INFILE[[:space:]]+' OR
		sqltext REGEXP '[[:space:]]*CREATE[[:space:]]+TRIGGER[[:space:]]+' OR
		sqltext REGEXP '[[:space:]]*CREATE[[:space:]]+DATABASE[[:space:]]+' OR
		sqltext REGEXP '[[:space:]]*DROP[[:space:]]+TRIGGER[[:space:]]+' OR
		sqltext REGEXP '[[:space:]]*DROP[[:space:]]+PROCEDURE[[:space:]]+' OR
		sqltext REGEXP '[[:space:]]*CREATE[[:space:]]+PROCEDURE[[:space:]]+'
	THEN
		CALL util.exec_outside(sqltext);
	ELSE
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
	END IF;
END;;


DROP PROCEDURE IF EXISTS util.dir_list;;
CREATE PROCEDURE util.dir_list(
	dirPath		VARCHAR(500),
	tableName	VARCHAR(80)
)
BEGIN
	CALL util.exec_outside(concat(
		"CREATE TABLE ", tableName, " AS SELECT * FROM DIRECTORY(",dirPath,")"
	));
END;;


###############################################################################
## PROVIDE A LIST OF STRICTLY INCREASING INTEGERS
## MAKES A TABLE OF RANGES BETWEEN THOSE INTEGERS
###############################################################################
DROP PROCEDURE IF EXISTS util.integer_range;;
CREATE PROCEDURE util.integer_range(
  table_name  VARCHAR(100),
	list		    VARCHAR(500)
)
BEGIN
  DECLARE v VARCHAR(500);
  DECLARE a VARCHAR(10);
  DECLARE b VARCHAR(10);
  CALL util.exec(concat("DROP TABLE IF EXISTS ",table_name), FALSE);
  CALL util.exec(concat("CREATE TABLE ",table_name," ( `min` INTEGER, `max` INTEGER)"), FALSE);

  SET v=list;
  SET b=string.get_word(list, ",", 0);
  SET v=substring(list, length(b)+2);
  allRange: LOOP
    SET a=b;
    SET b=string.get_word(v, ",", 0);
    IF b IS NULL OR b='' THEN LEAVE allRange; END IF;

    SET v=substring(v, length(b)+2);
    CALL util.exec(concat("INSERT INTO ",table_name," VALUES (",a,", ",b," )"), FALSE);
	END LOOP allRange;

END;;


CALL util.integer_range("temp_money", "1, 2, 5, 10, 20, 50, 100, 500");;
SELECT * FROM temp_money;;


###############################################################################
## SPECIFICALLY RUN SQL FROM THE CLIENT SIDE
###############################################################################
DROP PROCEDURE IF EXISTS util.exec_outside;;
CREATE PROCEDURE util.exec_outside(
	sqltext		VARCHAR(8000)
)
BEGIN
	DECLARE done int;
	DECLARE actual DECIMAL(20,10);
	DECLARE countdown INTEGER;
	DECLARE uid bigint;



	SET uid=util.newID();
	INSERT INTO util.exec_queue (id, request_time, sqltext) VALUES (
		uid,
		now(),
		sqltext
	);

	SET countdown=5*60/0.5;	## WAIT 5min AND THEN FAIL
	wait4sql: LOOP
		SELECT CASE WHEN response_time IS NOT NULL THEN 1 ELSE 0 END
		INTO done
		FROM util.exec_queue
		WHERE id=uid
		;

		IF done THEN LEAVE wait4sql; END IF;

		## WE SHOULD THROW AN ERROR
		SET countdown=countdown-1;
		IF countdown=0 THEN
			IF ignore_error THEN
				LEAVE wait4sql;
			ELSE
				CALL debug.error("Could not load data");
			END IF;
		END IF;

		SELECT sleep(0.5) INTO actual;
	END LOOP wait4sql;
END;;


DROP TABLE IF EXISTS util.getValue_table;;
CREATE TABLE util.getValue_table (
	value		VARCHAR(8000)
);;

DROP PROCEDURE IF EXISTS util.getValue;;
CREATE PROCEDURE util.getValue (
	OUT output	VARCHAR(8000),
	sqltext		VARCHAR(8000)
)
BEGIN
	DELETE FROM util.getValue_table;

	SET @temp=concat("INSERT INTO util.getValue_table ",sqltext);
	PREPARE stmt FROM @temp;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	SELECT value INTO output FROM util.getValue_table;

	#SELECT output;
END;;


CREATE TABLE IF NOT EXISTS util.uid_next(
	id 		BIGINT
);;

DROP FUNCTION IF EXISTS util.newID;;
CREATE FUNCTION util.newID ()
	RETURNS BIGINT
	READS SQL DATA
BEGIN
	IF @util_curr_id IS NULL THEN
		SELECT max(id) INTO @util_curr_id FROM util.uid_next;
		IF @util_curr_id IS NULL THEN
			INSERT INTO util.uid_next VALUES (0);
			SET @util_curr_id=0;
		END IF;
		UPDATE util.uid_next SET id=@util_curr_id+1000;
	ELSEIF mod(@util_curr_id, 1000)=0 THEN
		SELECT max(id) INTO @util_curr_id FROM util.uid_next;
		UPDATE util.uid_next SET id=@util_curr_id+1000;
	END IF;

	SET @util_curr_id=@util_curr_id+1;
	RETURN @util_curr_id-1;
END;;

