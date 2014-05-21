DELIMITER ;;

DROP DATABASE IF EXISTS json;;
CREATE DATABASE json;;
USE json;;


-- JSON GET OBJECT
-- RETURN THE JSON OBJECT REFERENCED BY TAG NAME
-- FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
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
		RETURN NULL;
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

SELECT json(" [\"results\": {}, junk]", "results") result, "{}" expected from dual;;
SELECT json(" [\"results\": {\"hi\":20}, junk]", "results") result, "{\"hi\":20}" expected from dual;;
SELECT json(" \"results\": {\"hi\":20}, junk]", "results") result, "{\"hi\":20}" expected  from dual;;
SELECT json(" [\"results\": {\"some thing\":[324,987]}, junk]", "results") result, "{\"some thing\":[324,987]}" expected  from dual;;
SELECT json(" \"results\": {\"some thing\":[324,987], {\"other\":\"99\\\"\"}}, jumk", "results") result, "{\"some thing\":[324,987], {\"other\":\"99\\\"\"}}" expected  from dual;;
SELECT json(" \"results\": {\"some thing\":[324,987], {\"other\":\"99\\\"}}, jumk", "results") result, "{\"some thing\":[324,987], {\"other\":\"99\\\"}}, jumk" expected  from dual;;


-- JSON GET STRING
-- RETURN STRING REFERENCED BY TAG VALUE
-- FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS string;;
CREATE FUNCTION string (
	value		longtext character set utf8,
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
		RETURN string.between(substring(value, instr(value, begin_tag)+length(begin_tag), 65000), "\"", "\"", 1);
	END IF;
END;;

-- JSON GET NUMBER
-- RETURN A NUMERIC VALUE REFERNCED BY TAG
-- FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS number;;
CREATE FUNCTION number (
	value		longtext character set utf8,
	tag			VARCHAR(40)
) RETURNS varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);
	DECLARE begin_data INTEGER;
	DECLARE end_data INTEGER;

	SET begin_tag=concat("\"", tag, "\":");
	SET begin_data=instr(value, begin_tag);
	IF begin_data=0 THEN
		RETURN NULL;
	ELSE
		SET begin_data=begin_data+length(begin_tag)-1;
		SET end_data=math.minof(string.locate(',', value, begin_data), string.locate('}', value, begin_data));
		RETURN trim(substring(value, begin_data+1, end_data-begin_data-1));
	END IF;
END;;

SELECT number(" \"results\": {\"last number\":324}, jumk", "last number") result, 324 expected from dual;;
SELECT number('{"count": 20, "skew": 1.4737744878964223, "unbiased": true, "variance": 5.211875000037253, "kurtosis": 2.6389621539338295, "mean": 715.775}', "mean") result, "715.775" expected from dual;;


-- JSON GET ARRAY
-- RETURN ARRAY REFERNCED BY TAG NAME
-- FOR NOW, ONLY ARRAYS OF PRIMITIVES CAN BE RETURNED
-- FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
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
		RETURN concat("[", string.between(substring(value, instr(value, begin_tag)+length(begin_tag)-1, 65000), "[", "]", 1), "]");
	END IF;
END;;


-- RETURN A NUMERIC VALUE AT ARRAY INDEX
-- FINDS FIRST INSTANCE OF AN ARRAY WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS arrayn;;
CREATE FUNCTION arrayn (
	value		VARCHAR(65000) character set latin1,
	index_		INTEGER
) RETURNS varchar(65000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	RETURN trim(string.get_word(string.between(value, "[", "]", 1), ",", index_));
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

