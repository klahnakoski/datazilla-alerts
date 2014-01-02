drop database if exists cnv;
create database cnv;
use cnv;
DELIMITER ;;




DROP FUNCTION IF EXISTS cnv.ip2integer;;
CREATE FUNCTION cnv.ip2integer(
	value varchar(15)
)
	RETURNS bigint
	NO SQL
    DETERMINISTIC
BEGIN
	RETURN INET_ATON(value);
END;;


###############################################################################
## CONVERT IP ADDRESS TO 4BYTE INTEGER
###############################################################################
DROP FUNCTION IF EXISTS cnv.ip2integer;;
CREATE FUNCTION cnv.ip2integer(
	value		VARCHAR(15)
)
	RETURNS decimal(10)
	NO SQL
	DETERMINISTIC
BEGIN
	RETURN
		cast(string_get_word(value, '.', 0) as decimal(10))*16777216+
		cast(string_get_word(value, '.', 1) as decimal(10))*65536+
		cast(string_get_word(value, '.', 2) as decimal(10))*256+
		cast(string_get_word(value, '.', 3) as decimal)
	;
END;;





DROP FUNCTION IF EXISTS cnv.binary2bigint;;
CREATE FUNCTION cnv.binary2bigint(
	value binary(8)
)
	RETURNS bigint
	NO SQL
    DETERMINISTIC
BEGIN
	DECLARE output DECIMAL(20);
	IF value IS NULL THEN RETURN NULL; END IF;
	SET output=CAST(CONV(hex(value), 16, 10) AS DECIMAL(20));
	IF output>9223372036854775808 THEN SET output=output-18446744073709551616; END IF;
	RETURN cast(output AS signed);
END;;

DROP FUNCTION IF EXISTS cnv.binary2integer;;
CREATE FUNCTION cnv.binary2integer(
	value binary(4)
)
	RETURNS bigint
	NO SQL
    DETERMINISTIC
BEGIN
	DECLARE output DECIMAL(11);
	IF value IS NULL THEN RETURN NULL; END IF;
	SET output=CAST(CONV(hex(value), 16, 10) AS DECIMAL(11));
	IF output>2147483648 THEN SET output=output-4294967296; END IF;
	RETURN cast(output AS signed);
END;;



DROP FUNCTION IF EXISTS cnv.varchar2hash;;
CREATE FUNCTION cnv.varchar2hash(
	value varchar(1000)
)
	RETURNS INTEGER
	NO SQL
    DETERMINISTIC
BEGIN
	RETURN cnv.binary2integer(right(md5(upper(string.trim(value))),4));
END;;



###############################################################################
## CONVERT TO QUOTED
###############################################################################
DROP FUNCTION IF EXISTS cnv.integer2quote;;
CREATE FUNCTION cnv.integer2quote(
	value BIGINT
)
	RETURNS VARCHAR(2000)
	NO SQL
    DETERMINISTIC
BEGIN
	IF value IS NULL THEN RETURN ''; END IF;
	RETURN cast(value AS CHAR);
END;;


DROP FUNCTION IF EXISTS cnv.datetime2quote;;
CREATE FUNCTION cnv.datetime2quote(
	value datetime
)
	RETURNS VARCHAR(2000)
	NO SQL
    DETERMINISTIC
BEGIN
	IF value IS NULL THEN RETURN ''; END IF;
	RETURN date_format(value,'"%d-%b-%Y %H:%i:%s"');
END;;



DROP FUNCTION IF EXISTS cnv.varchar2quote;;
CREATE FUNCTION cnv.varchar2quote(
	value varchar(2000)
)
	RETURNS VARCHAR(2000)
	NO SQL
    DETERMINISTIC
BEGIN
	DECLARE temp VARCHAR(2000);

	IF locate('\n', value)>0 OR locate('\"', value)>0 OR locate('\t', value)>0 THEN
		SET temp=
		concat(
			"\"",
			replace(
			replace(
			replace(
				value,
				'\n', '\\n'),
				'\"', '\\\"'),
				'\t', '\\t'),
			"\""
			)
		;
	ELSE
		SET temp=value;
	END IF;

	IF value IS NULL THEN RETURN ''; END IF;
	RETURN temp;
END;;


DROP FUNCTION IF EXISTS cnv.varchar2long;;
CREATE FUNCTION cnv.varchar2long(
	value varchar(2000)
)
	RETURNS LONG
	NO SQL
    DETERMINISTIC
BEGIN
	IF (value IS NULL) THEN RETURN -1; END IF;
	CALL debug.`log`(value);
	RETURN convert(value, DECIMAL(20));
END;;



###############################################################################
## CONVERT TO SQL
###############################################################################
DROP FUNCTION IF EXISTS cnv.integer2sql;;
CREATE FUNCTION cnv.integer2sql(
	value BIGINT
)
	RETURNS VARCHAR(2000)
	NO SQL
    DETERMINISTIC
BEGIN
	IF value IS NULL THEN RETURN 'null'; END IF;
	RETURN cast(value AS CHAR);
END;;


DROP FUNCTION IF EXISTS cnv.date2sql;;
DROP FUNCTION IF EXISTS cnv.datetime2sql;;
CREATE FUNCTION cnv.datetime2sql(
	value datetime
)
	RETURNS VARCHAR(2000)
	NO SQL
    DETERMINISTIC
BEGIN
	IF value IS NULL THEN RETURN 'null'; END IF;
	RETURN concat("str_to_date('",date_format(value,'%Y%m%d%H%i%s'),"','%Y%m%d%H%i%s')");
END;;



DROP FUNCTION IF EXISTS cnv.varchar2sql;;
CREATE FUNCTION cnv.varchar2sql(
	value varchar(2000)
)
	RETURNS VARCHAR(2000)
	NO SQL
    DETERMINISTIC
BEGIN
	IF value IS NULL THEN RETURN 'null'; END IF;
	RETURN concat(
		"\"",
		replace(
		replace(
		replace(
			value,
			'\n', '\\n'),
			'\"', '\\\"'),
			'\t', '\\t'),
		"\""
		);
END;;

###############################################################################
## CONVERT TO TAB DELIMITED SO COMMAS IN QUOTES ARE NOT NEEDED (TABS ARE REMOVED)
## AND QUOTES ARE REMOVED
###############################################################################
DROP FUNCTION IF EXISTS cnv.csv2tab;;
CREATE FUNCTION cnv.csv2tab(
	value		VARCHAR(60000) character set latin1
)
	RETURNS VARCHAR(60000) character set latin1
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE output VARCHAR(60000) character set latin1;
	DECLARE mode INTEGER;
	DECLARE c VARCHAR(1);
	DECLARE i INTEGER;
	DECLARE l INTEGER;


	SET mode=0;   # NOT IN QUOTE
	SET output='';
	SET i=1;
	SET l=length(value);

	WHILE(i<=l) DO
		SET c=substring(value, i, 1);
		IF (c='\t') THEN
			SET c=' ';
		ELSEIF (c=',' AND mode=0) THEN
			SET c='\t';
		ELSEIF (c='\"') THEN  #WE WILL IGNORE ESCAPING FOR NOW
			SET c='';
			SET mode=(mode+1) mod 2;
		END IF;
		SET output=concat(output, c);
		SET i=i+1;
	END WHILE;
	RETURN output;
END;;





## WE ASSUME THE LONG IS A UNIX DATESTAMP IN MILLISECONDS
DROP FUNCTION IF EXISTS cnv.long2datetime;;
CREATE FUNCTION cnv.long2datetime(
	value 	LONG
)
	RETURNS DATETIME
	NO SQL
	DETERMINISTIC
BEGIN
	RETURN FROM_UNIXTIME(value/1000);
END;;









DELIMITER;
