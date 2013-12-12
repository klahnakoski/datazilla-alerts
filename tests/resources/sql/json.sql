
DELIMITER ;;

DROP DATABASE IF EXISTS json;;
CREATE DATABASE json;;
USE json;;


# JSON GET OBJECT
# RETURN THE JSON OBJECT REFERENCED BY TAG NAME
# FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS json;;
CREATE FUNCTION json (
	value		VARCHAR(65000) character set latin1,
	tag			VARCHAR(40)
) RETURNS
	varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s DECIMAL(10,0);
	DECLARE i DECIMAL(10,0);
	DECLARE c CHAR;
	DECLARE d INTEGER; # DEPTH
	DECLARE begin_tag VARCHAR(50);

	SET begin_tag=concat("\"", tag, "\":");
	SET s=locate(begin_tag, value);
	IF s=0 THEN
		RETURN begin_tag;
	ELSE
		SET s=locate("{", value, s+length(begin_tag));
		SET i=s+1;
		SET d=1;
		DD: LOOP
			SET c=substring(value, i, 1);
			IF c="\"" THEN
				SET i=i+1;
				QQ: LOOP
					SET c=substring(value, i, 1);
					IF c="\\" THEN
						SET i=i+1;
					ELSEIF c="\"" THEN
						LEAVE QQ;
					END IF;
					SET i=i+1;
					IF i>length(value) OR i-s>65000 THEN LEAVE DD; END IF;
				END LOOP QQ;
			ELSEIF c="{" OR c="[" THEN
				SET d=d+1;
			ELSEIF c="}" OR c="]" THEN
				SET d=d-1;
			END IF;
			SET i=i+1;
			IF d=0 OR i>length(value) OR i-s>65000 THEN LEAVE DD; END IF;
		END LOOP DD;
		RETURN substring(value, s, i-s);
	END IF;
END;;


SELECT json(" [\"results\": {}, junk]", "results") from dual;;
SELECT json(" [\"results\": {\"hi\":20}, junk]", "results") from dual;;
SELECT json(" \"results\": {\"hi\":20}, junk]", "results") from dual;;
SELECT json(" [\"results\": {\"some thing\":[324,987]}, junk]", "results") from dual;;
SELECT json(" \"results\": {\"some thing\":[324,987], {\"other\":\"99\\\"\"}}, jumk", "results") from dual;;
SELECT json(" \"results\": {\"some thing\":[324,987], {\"other\":\"99\\\"}}, jumk", "results") from dual;;


# JSON GET STRING
# RETURN STRING REFERENCED BY TAG VALUE
# FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS string;;
CREATE FUNCTION string (
	value		VARCHAR(65000) character set latin1,
	tag			VARCHAR(40)
) RETURNS varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);

	SET begin_tag=concat("\"", tag, "\":");
	IF instr(value, begin_tag)=0 THEN
		RETURN NULL;
	ELSE
		RETURN string_between(substring(value, instr(value, begin_tag)+length(begin_tag), 65000), "\"", "\"", 1);
	END IF;
END;;

# JSON GET NUMBER
# RETURN A NUMERIC VALUE REFERNCED BY TAG
# FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS number;;
CREATE FUNCTION number (
	value		VARCHAR(65000) character set latin1,
	tag			VARCHAR(40)
) RETURNS varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);

	SET begin_tag=concat("\"", tag, "\":");
	IF instr(value, begin_tag)=0 THEN
		RETURN NULL;
	ELSE
		RETURN string_between(substring(value, instr(value, begin_tag)+length(begin_tag)-1, 65000), ":", ",", 1);
	END IF;
END;;

# JSON GET ARRAY
# RETURN ARRAY REFERNCED BY TAG NAME
# FOR NOW, ONLY ARRAYS OF PRIMITIVES CAN BE RETURNED
# FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS array;;
CREATE FUNCTION array (
	value		VARCHAR(65000) character set latin1,
	tag			VARCHAR(40)
) RETURNS
	varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);

	SET begin_tag=concat("\"", tag, "\":");
	IF instr(value, begin_tag)=0 THEN
		RETURN NULL;
	ELSE
		RETURN concat("[", string_between(substring(value, instr(value, begin_tag)+length(begin_tag)-1, 65000), "[", "]", 1), "]");
	END IF;
END;;


# RETURN A NUMERIC VALUE AT ARRAY INDEX
# FINDS FIRST INSTANCE OF AN ARRAY WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS arrayn;;
CREATE FUNCTION arrayn (
	value		VARCHAR(65000) character set latin1,
	index_		INTEGER
) RETURNS varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	RETURN trim(string_get_word(string_between(value, "[", "]", 1), ",", index_));
END;;



DROP FUNCTION IF EXISTS string_word_count;;
CREATE FUNCTION string_word_count(
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


DROP FUNCTION IF EXISTS string_get_word;;
CREATE FUNCTION string_get_word(
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


DROP FUNCTION IF EXISTS string_between;;
CREATE FUNCTION string_between(
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


DROP FUNCTION IF EXISTS slice;;
CREATE FUNCTION slice(
	value		VARCHAR(65000) character set latin1,
	start_		INTEGER,
	end_		INTEGER
)
	RETURNS VARCHAR(65000) character set latin1
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE n INTEGER;
	DECLARE s INTEGER;
	DECLARE e INTEGER;

	IF end_=start_ THEN RETURN "[]"; END IF;

	SET n=start_;
	SET s=LOCATE("[", value)+1;
	ls: LOOP
		IF n=0 THEN
			LEAVE ls;
		END IF;
		SET s=LOCATE(",", value, s)+1;
		IF s=0 THEN
			RETURN "[]";
		END IF;
		SET n=n-1;
	END LOOP ls;

	SET n=end_-start_;
	SET e=s-1;
	le: LOOP
		IF n=0 THEN
			RETURN concat("[", trim(substring(value, s, e-s)), "]");
		END IF;
		SET e=LOCATE(",", value, e+1);
		IF e=0 THEN
			SET e=LOCATE("]", value, s);
			RETURN concat("[", substring(value, s, e-s), "]");
		END IF;
		SET n=n-1;
	END LOOP;
END;;

SELECT slice("[23, 45, 32, 44, 99]", 1,3) from dual;;
SELECT slice("[23, 45, 32, 44, 99]", 0,3) from dual;;
SELECT slice("[23, 45, 32, 44, 99]", 0,0) from dual;;
SELECT slice("[23, 45, 32, 44, 99]", 0,9) from dual;;



# TAKE JSON ARRAY OF NUMBERS AND RETURN ARRAY OF CENTERED STATS
DROP FUNCTION IF EXISTS math_stats;;
CREATE FUNCTION math_stats(
	value		VARCHAR(65000) character set latin1
)
	RETURNS VARCHAR(200)
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE e INTEGER;
	DECLARE z0 DOUBLE;
	DECLARE z1 DOUBLE;
	DECLARE z2 DOUBLE;
	DECLARE v VARCHAR(20);

	SET z0=0;
	SET z1=0;
	SET z2=0;
	SET s=LOCATE("[", value)+1;
	LOOP
		SET e=LOCATE(",", value, s);
		IF (e=0) THEN
			SET e=LOCATE("]", value, s);
			SET v=trim(substring(value, s, e-s));
			IF length(v)=0 AND z0=0 THEN RETURN null; END IF;
			SET z2=z2+(v*v);
			SET z1=z1+v;
			SET z0=z0+1;
			RETURN concat("[",z0,",",z1/z0,",",(z2-((z1*z1)/z0))/z0, "]");
		END IF;
		SET v=trim(substring(value, s, e-s));
		SET z2=z2+(v*v);
		SET z1=z1+v;
		SET z0=z0+1;
		SET s=e+1;
	END LOOP;
END;;

SELECT math_stats("[32,56,38,45,30]") FROM dual;;

