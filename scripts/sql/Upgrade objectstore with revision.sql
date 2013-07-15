
DELIMITER ;;
use ekyle_objectstore_1;;

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


UPDATE objectstore 
SET revision=string_between(substring(json_blob, locate("revision\": \"", json_blob), 40), "revision\": \"", "\",", 1)
WHERE revision IS NULL
;;

CREATE INDEX objectstore_revision ON objectstore(revision);;

select count(1) from objectstore where revision IS NULL
