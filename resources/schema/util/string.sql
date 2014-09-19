drop database if exists string;
create database string;
use string;
DELIMITER ;;


DROP FUNCTION IF EXISTS string.deformat;;
CREATE FUNCTION string.deformat(
	value 		VARCHAR(2000)
) RETURNS varchar(2000)
	NO SQL
    DETERMINISTIC
BEGIN
	DECLARE i INTEGER;
	DECLARE c CHAR(1);
	DECLARE output VARCHAR(2000);

	IF value IS NULL THEN RETURN NULL; END IF;

	SET output='';
	SET i=1;
	WHILE (i<=length(value)) DO BEGIN
		SET c=substring(value, i, 1);
		IF (c>='a' AND c<='z') OR (c>='0' AND c<='9') OR (c>='A' AND c<='Z') THEN
			SET output = concat(output, c);
		END IF;
		SET i=i+1;
	END; END WHILE;
	RETURN output;
END;;


DROP FUNCTION IF EXISTS string.remove_control;;
CREATE FUNCTION string.remove_control(
	value 		VARCHAR(2000)
) RETURNS varchar(2000)
	NO SQL
    DETERMINISTIC
BEGIN
	DECLARE i INTEGER;
	DECLARE c CHAR(1);
	DECLARE output VARCHAR(2000);

	IF value IS NULL THEN RETURN NULL; END IF;

	SET output='';
	SET i=1;
	WHILE (i<=length(value)) DO BEGIN
		SET c=substring(value, i, 1);
		IF c<' ' THEN
			SET output = concat(output, ' ');
		ELSE
			SET output = concat(output, c);
		END IF;
		SET i=i+1;
	END; END WHILE;
	RETURN output;
END;;


DROP FUNCTION IF EXISTS string.get_word;;
CREATE FUNCTION string.get_word(
	value		VARCHAR(65000) character set latin1,
	delimiter	VARCHAR(300),
	num			INTEGER
)
	RETURNS VARCHAR(65000) character set latin1
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE n INTEGER;
	DECLARE e INTEGER;
	DECLARE s INTEGER;

	SET n=0;
	SET e=0;
	SET s=1;
	l1: LOOP
		SET e=LOCATE(delimiter, value, s);
		IF (e=0) THEN
			#NO MORE DELIMITERS
			SET e=length(value)+1;
			IF n<num THEN RETURN ''; END IF;
		END IF;
		IF n=num THEN RETURN mid(value, s, e-s); END IF;
		SET n=n+1;
		SET s=e+length(delimiter);
	END LOOP l1;
END;;


-- RETURN POSITION OF find, OR RETURN length(value)+1 IF NOT FOUND
DROP FUNCTION IF EXISTS string.locate;;
CREATE FUNCTION string.locate(
	find	VARCHAR(300) character set latin1,
	value	longtext,
	start   INTEGER
) RETURNS 
	INTEGER
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE e INTEGER;
	SET e=LOCATE(find, value, start);
	IF e=0 THEN RETURN length(value)+1; END IF;
	RETURN e;
END;;




DROP FUNCTION IF EXISTS string.between;;
CREATE FUNCTION string.between(
	value		mediumtext,
	begin_tag	VARCHAR(40),
	end_tag		VARCHAR(40),
	start_index	INTEGER
)
	RETURNS mediumtext
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




DROP FUNCTION IF EXISTS string.is_numeric;;
CREATE FUNCTION string.is_numeric(
	value		VARCHAR(80)
)
	RETURNS TINYINT(1)
	NO SQL
	DETERMINISTIC
BEGIN
	RETURN value REGEXP '^(-|\\+)?([0-9]+\\.[0-9]*|[0-9]*\\.[0-9]+|[0-9]+)$';
END;;


DROP FUNCTION IF EXISTS string.ltrim;;
CREATE FUNCTION string.ltrim(
	value		VARCHAR(60000) character set latin1
)
	RETURNS VARCHAR(60000) character set latin1
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE i INTEGER;
	IF value IS NULL THEN RETURN NULL; END IF;

	SET i=1;
	WHILE (i<=length(value) AND substring(value, i, 1)<=' ') DO
		SET i=i+1;
	END WHILE;
	RETURN substring(value, i);
END;;



DROP FUNCTION IF EXISTS string.rtrim;;
CREATE FUNCTION string.rtrim(
	value		VARCHAR(60000) character set latin1
)
	RETURNS VARCHAR(60000) character set latin1
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE i INTEGER;
	IF value IS NULL THEN RETURN NULL; END IF;

	SET i=length(value);
	WHILE (i>=1 AND substring(value, i, 1)<=' ') DO
		SET i=i-1;
	END WHILE;
	RETURN substring(value, 1, i);
END;;


DROP FUNCTION IF EXISTS string.trim;;
CREATE FUNCTION string.trim(
	value		VARCHAR(60000) character set latin1
)
	RETURNS VARCHAR(60000) character set latin1
	NO SQL
	DETERMINISTIC
BEGIN
	RETURN string.ltrim(string.rtrim(value));
END;;



DROP FUNCTION IF EXISTS string.word_count;;
CREATE FUNCTION string.word_count(
	value		VARCHAR(65000) character set latin1,
	delimiter	VARCHAR(300)
)
	RETURNS INTEGER
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE n INTEGER;

	SET n=1;
	SET s=1;
	LOOP
		SET s=LOCATE(delimiter, value, s);
		IF (s=0) THEN
			RETURN n;
		END IF;
		SET n=n+1;
		SET s=s+length(delimiter);
	END LOOP;
END;;




DELIMITER ;
