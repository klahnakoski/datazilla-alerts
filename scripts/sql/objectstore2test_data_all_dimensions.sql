#PARSE OUT TEST 

DELIMITER ;;

USE ekyle_perftest_1;;


DROP TABLE IF EXISTS util_digits;;
CREATE TABLE util_digits (
	digit  DECIMAL(1)
);;
INSERT INTO util_digits VALUES (0);;
INSERT INTO util_digits VALUES (1);;
INSERT INTO util_digits VALUES (2);;
INSERT INTO util_digits VALUES (3);;
INSERT INTO util_digits VALUES (4);;
INSERT INTO util_digits VALUES (5);;
INSERT INTO util_digits VALUES (6);;
INSERT INTO util_digits VALUES (7);;
INSERT INTO util_digits VALUES (8);;
INSERT INTO util_digits VALUES (9);;




-- {
--     "test_machine" : {
--         "platform"  : "x86_64",
--         "osversion" : "OS X 10.8",
--         "os"        : "mac",
--         "name"      : "talos-mtnlion-r5-067"
--     },
--     "testrun" : {
--         "date"    : 1370501293,
--         "suite"   : "tspaint_places_generated_med",
--         "options" : {
--             "responsiveness"  : false,
--             "tpmozafterpaint" : false,
--             "tpchrome"        : true,
--             "tppagecycles"    : 1,
--             "tpcycles"        : 10,
--             "tprender"        : false,
--             "shutdown"        : false,
--             "extensions"      : [{"name" : "pageloader@mozilla.org"}],
-- 			"rss"             : false
--         }
--     },
--     "results" : {
--         "tspaint_places_generated_med" : [996.0,770.0,786.0,883.0,745.0,734.0,788.0,866.0,695.0,711.0,713.0,676.0,699.0,693.0,675.0,682.0,698.0,747.0,770.0,788.0]
--     },
--     "test_build" : {
--         "version"  : "24.0a1",
--         "revision" : "fd3eb4b73292",
--         "name"     : "Firefox",
--         "branch"   : "Mozilla-Inbound",
--         "id"       : "20130605230639"
--     }
-- }





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
DROP FUNCTION IF EXISTS json_s;;
CREATE FUNCTION json_s (
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
DROP FUNCTION IF EXISTS json_n;;
CREATE FUNCTION json_n (
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
DROP FUNCTION IF EXISTS json_a;;
CREATE FUNCTION json_a (
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
		RETURN concat("[", string_between(substring(value, instr(value, begin_tag)+length(begin_tag)-1, 65000), "[", "]"), "]");
	END IF;	
END;;	


# RETURN A NUMERIC VALUE AT ARRAY INDEX
# FINDS FIRST INSTANCE OF AN ARRAY WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS json_an;;
CREATE FUNCTION json_an (
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



DROP FUNCTION IF EXISTS json_substring;;
CREATE FUNCTION json_substring(
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

SELECT json_substring("[23, 45, 32, 44, 99]", 1,3) from dual;;
SELECT json_substring("[23, 45, 32, 44, 99]", 0,3) from dual;;
SELECT json_substring("[23, 45, 32, 44, 99]", 0,0) from dual;;
SELECT json_substring("[23, 45, 32, 44, 99]", 0,9) from dual;;



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



ALTER TABLE test_data_all_dimensions DROP FOREIGN KEY fk_test_run_id_tdad;;
ALTER TABLE test_data_all_dimensions MODIFY page_url varchar(255) NULL DEFAULT NULL;
ALTER TABLE test_data_all_dimensions MODIFY mean double NULL DEFAULT NULL;
ALTER TABLE test_data_all_dimensions MODIFY std double NULL DEFAULT NULL;


SELECT count(1) FROM test_data_all_dimensions;;
DELETE FROM test_data_all_dimensions;;




