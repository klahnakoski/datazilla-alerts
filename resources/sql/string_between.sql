DELIMITER;;


use ekyle_perftest_1;;


DROP FUNCTION IF EXISTS ekyle_objectstore_1.string_between;;
CREATE FUNCTION ekyle_objectstore_1.`string_between`(
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
