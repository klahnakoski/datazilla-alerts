#PARSE OUT TEST 

DELIMITER ;;

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



USE ekyle_perftest_1;;

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
	value		BLOB,
	tag			VARCHAR(40)
) RETURNS varchar(60000) 
	CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);
	
	SET begin_tag=concat("\"", tag, "\":");
	IF instr(value, begin_tag)=0 THEN 
		RETURN NULL;
	ELSE 
		RETURN substring(value, instr(value, begin_tag)+length(begin_tag), 60000);
	END IF;
END;;	

# JSON GET STRING
# RETURN STRING REFERENCED BY TAG VALUE 
# FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS json_s;;
CREATE FUNCTION json_s (
	value		VARCHAR(60000) character set latin1,
	tag			VARCHAR(40)
) RETURNS varchar(60000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);
	
	SET begin_tag=concat("\"", tag, "\":");
	IF instr(value, begin_tag)=0 THEN 
		RETURN NULL;
	ELSE 
		RETURN string_between(substring(value, instr(value, begin_tag)+length(begin_tag), 60000), "\"", "\"", 1);
	END IF;	
END;;	

# JSON GET NUMBER
# RETURN A NUMERIC VALUE REFERNCED BY TAG
# FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS json_n;;
CREATE FUNCTION json_n (
	value		VARCHAR(60000) character set latin1,
	tag			VARCHAR(40)
) RETURNS varchar(60000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);
	
	SET begin_tag=concat("\"", tag, "\":");
	IF instr(value, begin_tag)=0 THEN 
		RETURN NULL;
	ELSE 
		RETURN string_between(substring(value, instr(value, begin_tag)+length(begin_tag)-1, 60000), ":", ",", 1);
	END IF;	
END;;	

# JSON GET ARRAY
# RETURN ARRAY REFERNCED BY TAG NAME
# FOR NOW, ONLY ARRAYS OF PRIMITIVES CAN BE RETURNED
# FINDS FIRST INSTANCE WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS json_a;;
CREATE FUNCTION json_a (
	value		VARCHAR(60000) character set latin1,
	tag			VARCHAR(40)
) RETURNS 
	varchar(60000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	DECLARE s INTEGER;
	DECLARE begin_tag VARCHAR(50);
	
	SET begin_tag=concat("\"", tag, "\":");
	IF instr(value, begin_tag)=0 THEN 
		RETURN NULL;
	ELSE 
		RETURN concat("[", string_between(substring(value, instr(value, begin_tag)+length(begin_tag)-1, 60000), "[", "]"), "]");
	END IF;	
END;;	


# RETURN A NUMERIC VALUE AT ARRAY INDEX
# FINDS FIRST INSTANCE OF AN ARRAY WITH NO REGARD FOR DEPTH
DROP FUNCTION IF EXISTS json_an;;
CREATE FUNCTION json_an (
	value		VARCHAR(60000) character set latin1,
	index_		INTEGER
) RETURNS varchar(60000) CHARSET latin1
    NO SQL
    DETERMINISTIC
BEGIN
	RETURN trim(string_get_word(string_between(value, "[", "]", 1), ",", index_));
END;;	



DROP FUNCTION IF EXISTS string_word_count;;
CREATE FUNCTION string_word_count(
	value		VARCHAR(60000) character set latin1,
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
	value		VARCHAR(60000) character set latin1,
	delimiter	VARCHAR(300),
	num			INTEGER
) 
	RETURNS VARCHAR(60000) character set latin1
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
	value		VARCHAR(60000) character set latin1,
	start_		INTEGER,
	end_		INTEGER
) 
	RETURNS VARCHAR(60000) character set latin1
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
	value		VARCHAR(60000) character set latin1
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
SELECT count(1) FROM test_data_all_dimensions;;
DELETE FROM test_data_all_dimensions;;




-- ONGOING

USE ekyle_perftest_1;;


UPDATE ekyle_objectstore_1.objectstore
SET test_run_id=ekyle_perftest_1.util_newid()
WHERE test_run_id IS NULL
;;


-- SELECT string_between(json_blob, "results",  FROM objectstore
INSERT INTO test_data_all_dimensions (
	`test_run_id`, 
	`product_id`, 
	`operating_system_id`, 
	`test_id`, 
	`page_id`, 
	`date_received`, 
	`revision`, 
	`product`, 
	`branch`, 
	`branch_version`, 
	`operating_system_name`, 
	`operating_system_version`, 
	`processor`, 
	`build_type`, 
	`machine_name`, 
	`pushlog_id`, 
	`push_date`, 
	`test_name`, 
	`page_url`, 
	`mean`, 
	`std`, 
	`n_replicates`
)
SELECT STRAIGHT_JOIN 
	a.test_run_id, 
	0 `product_id`, 
	0 `operating_system_id`, 
	0 `test_id`, 
	0 `page_id`, 
	a.date_received,
	a.revision,
 	a.product,
 	a.branch,
 	a.branch_version,
 	a.operating_system_name,
 	a.operating_system_version,
 	a.processor,
	a.build_type,
	a.machine_name,
 	a.`pushlog_id`, 
 	a.`push_date`, 
 	a.test_name,
	string_between(                                string_get_word(results, "]", d1.digit*10+d2.digit), "\"", "\"", 1) page_url,
	      json_an(math_stats(json_substring(concat(string_get_word(results, "]", d1.digit*10+d2.digit), "]"),5,100)), 1) mean,
	round(json_an(math_stats(json_substring(concat(string_get_word(results, "]", d1.digit*10+d2.digit), "]"),5,100)), 2), 2) std,
	      json_an(math_stats(json_substring(concat(string_get_word(results, "]", d1.digit*10+d2.digit), "]"),5,100)), 0) `n_replicates`
FROM ( #RECORDS FOR THE BATTERY OF TESTS
	SELECT STRAIGHT_JOIN 
		tdad.test_run_id `test_run_id`,
	-- 	b.product_id `product_id`, 
	-- 	o.id `operating_system_id`, 
	-- 	tr.test_id `test_id`, 
	-- 	tpm.page_id `page_id`, 

		json_n(json(json_blob, "testrun"), "date") date_received,

		json_s(json(json_blob, "test_build"), "revision") revision,
	 	json_s(json(json_blob, "test_build"), "name") product,
	 	json_s(json(json_blob, "test_build"), "branch") branch,
	 	json_s(json(json_blob, "test_build"), "version") branch_version,
	 	json_s(json(json_blob, "test_machine"), "os") operating_system_name,
	 	json_s(json(json_blob, "test_machine"), "osversion") operating_system_version,
	 	json_s(json(json_blob, "test_machine"), "platform") processor,
		CASE WHEN instr(bm.alt_name, "Non-PGO") THEN "non" ELSE "opt" END build_type,
		json_s(json(json_blob, "test_machine"), "name") machine_name,
	 	pl.id `pushlog_id`, 
	 	pl.date `push_date`, 
	 	json_s(json(json_blob, "testrun"), "suite") test_name,
		## PREPROCESSING FOR NEXT RUN
		string_word_count(json(json_blob, "results"), "],") num_results,
		json(json_blob, "results") results
	FROM
		ekyle_objectstore_1.objectstore o
	LEFt JOIN
		ekyle_perftest_1.test_data_all_dimensions AS tdad ON tdad.test_run_id=o.test_run_id
	LEFT JOIN 
		pushlog_hgmozilla_1.changesets AS ch ON ch.revision=json_s(json(o.json_blob, "test_build"), "revision")
	LEFT JOIN
		pushlog_hgmozilla_1.pushlogs AS pl ON pl.id = ch.pushlog_id 
	LEFT JOIN 
		pushlog_hgmozilla_1.branches AS br ON pl.branch_id = br.id 
	LEFT JOIN 
		pushlog_hgmozilla_1.branch_map AS bm ON br.name = bm.name 
	WHERE
		o.test_run_id IS NOT NULL AND 
		tdad.test_run_id IS NULL AND
		left(json_s(json(json_blob, "testrun"), "suite"), 3)="tp5"
	) a
JOIN
	util_digits d1 ON (d1.digit+1)*10<a.num_results
JOIN
	util_digits d2 ON 1=1
WHERE
	d1.digit*10+d2.digit<a.num_results
;;


