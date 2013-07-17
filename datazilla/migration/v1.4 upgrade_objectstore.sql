
DELIMITER ;;
use ekyle_objectstore_1;;


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




DROP FUNCTION IF EXISTS string_between;;
CREATE FUNCTION string_between(
	value		VARCHAR(60000) character set latin1,
	begin_tag	VARCHAR(20),
	end_tag		VARCHAR(20),
	start_index	INTEGER 
) 
	RETURNS VARCHAR(60000) character set latin1
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





ALTER TABLE objectstore ADD COLUMN revision VARCHAR(12);;

#THE OBJECTSTORE WILL DICTATE THE test_run_id
UPDATE objectstore
SET test_run_id=ekyle_perftest_1.util_newid()
WHERE test_run_id IS NULL
;;


UPDATE objectstore 
SET revision=string_between(substring(json_blob, locate("revision\": \"", json_blob), 40), "revision\": \"", "\",", 1)
WHERE revision IS NULL
;;



CREATE INDEX objectstore_revision ON objectstore(revision);;

select count(1) from objectstore where revision IS NULL;;
