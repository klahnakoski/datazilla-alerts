drop database if exists export;
create database export;
use export;
SET SESSION binlog_format = 'ROW';

DELIMITER ;;

DROP PROCEDURE IF EXISTS export.exec;;
CREATE PROCEDURE export.exec (
	sqltext		VARCHAR(8000)
)
BEGIN
	CALL debug.log(sqltext);
	SET @temp=sqltext;
	PREPARE stmt FROM @temp;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END;;


DROP PROCEDURE IF EXISTS export.sql;;
CREATE PROCEDURE export.sql (
	sqlText		VARCHAR(60000) character set latin1,
	filename	VARCHAR(300),
	delimiter	VARCHAR(30)
) BEGIN
	DECLARE tableName VARCHAR(100);

	SET tableName=concat("temp_",convert(util.newID(), CHAR));

	##CREATE TABLE FROM SQL
	CALL util_exec(concat(
		"CREATE TABLE ", tableName, " AS ", sqlText
	), 0);

	CALL export.table('export', tableName, filename, delimiter);

	##DROP TABLE
	CALL util.exec(concat("DROP TABLE ", tableName), 0);
END;;



DROP PROCEDURE IF EXISTS export.table;;
CREATE PROCEDURE export.table (
	schemaname	VARCHAR(300),
	tablename	VARCHAR(300),
	filename	VARCHAR(300),
	delimiter 	VARCHAR(30)
) BEGIN
	DECLARE header VARCHAR(2000);
	DECLARE detail VARCHAR(8000);

	##REFLECT ON COLUMN TYPES AND NAMES
	CALL export.header(header, schemaname, tablename, delimiter);
	CALL export.detail(detail, schemaname, tablename, delimiter);

	##SEND TO FILE
	CALL export.exec(concat(
		"SELECT ", header, " UNION ",
		"SELECT  ",
		"	",detail," ",
		"INTO 	 ",
		"	OUTFILE '",filename,"'  ",
		"	FIELDS ",
		"		ENCLOSED BY '' ",  ##WORKAROUND FOR BUG http://bugs.mysql.com/bug.php?id=58165 IN MySQL VERSION 5.1.53
		"		ESCAPED BY '' ",
		"	LINES  ",
		"		STARTING BY '' ",  ##WORKAROUND FOR BUG http://bugs.mysql.com/bug.php?id=58165 IN MySQL VERSION 5.1.53
		"		TERMINATED BY '\\r\\n' ",
		"FROM ",
		"	",schemaName,".",tableName
	));

END;;







## OUTPUT COLUMNS AS A SINGLE DELIMITED STRING
DROP PROCEDURE IF EXISTS export.detail;;
CREATE PROCEDURE export.detail(
	OUT output VARCHAR(8000),
	schemaName	VARCHAR(300),
	tableName	VARCHAR(300),
	delimiter 	VARCHAR(30)
)
BEGIN
	SELECT
		group_concat(
			concat(
				CASE
				WHEN string.get_Word(column_type, "(", 0) IN ('char', 'text', 'varchar') THEN "cnv.varchar2quote(`"
				WHEN string.get_Word(column_type, "(", 0) IN ('date', 'datetime') THEN "cnv.datetime2quote(`"
				ELSE "cnv.integer2quote(`"
				END,
				column_name, "`),",cnv.varchar2sql(delimiter)
			)
			ORDER BY c.ordinal_position
			SEPARATOR ','
		)
	INTO
		output
	FROM
		information_schema.columns c
	WHERE
		table_schema=schemaName AND
		table_name=tablename
	;

	SET output=concat('concat(', left(output, length(output)-1-length(cnv.varchar2sql(delimiter))), ')');
END;;


## MAKE THE HEADER, DELIMITED, FROM THE COLUMN LIST
DROP PROCEDURE IF EXISTS export.header;;
CREATE PROCEDURE export.header (
	OUT output		VARCHAR(2000),
	schemaName		VARCHAR(300),
	tableName		VARCHAR(300),
	delimiter		VARCHAR(30)
)
	READS SQL DATA
BEGIN
	DECLARE isOK INTEGER;

	SELECT count(1) INTO isOK FROM information_schema.tables WHERE table_name=tablename AND table_schema=schemaname;
	IF (isOK=0) THEN
		CALL debug_error(concat("Expecting a table with name ",schemaname, '.', tablename));
	END IF;

	## group_concat CAN NOT ACCEPT VARIABLES, SO SEND THE SQL
	CALL util.getValue(output, concat(
		"SELECT ",
		"	group_concat(cnv_varchar2quote(column_name) ORDER BY c.ordinal_position SEPARATOR ",cnv_varchar2sql(delimiter), ") ",
		"FROM ",
		"	information_schema.columns c ",
		"WHERE ",
		"	table_name=", cnv.varchar2sql(tablename)," AND ",
		"	table_schema=", cnv.varchar2sql(schemaname)
		));

	SET output=cnv.varchar2sql(output);
END;;



###############################################################################
## EXPECTING TABLE THAT HAS "FILENAME" AND "BLOB" COLUMNS
###############################################################################
DROP PROCEDURE IF EXISTS export.blobs;;
CREATE PROCEDURE export.blobs (
	tablename		VARCHAR(300)
)
BEGIN
	CALL util.exec("DROP PROCEDURE IF EXISTS temp.export_blob", false);
	CALL util.exec(concat(
		"CREATE PROCEDURE temp.export_blob () ",
		"	READS SQL DATA ",
		"BEGIN ",
		"	DECLARE done INT DEFAULT 0; ",
		"	DECLARE filename_  VARCHAR(1000); ",
		"	DECLARE cur_export_blob CURSOR FOR SELECT filename FROM ", tablename, "; ",
		"	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1; ",

		"	OPEN cur_export_blob; ",
		"	load_loop: LOOP ",
		"		FETCH cur_export_blob INTO filename_; ",
		"		IF done THEN LEAVE load_loop; END IF; ",

		"		CALL util.exec_outside(concat( ",
		"			\"SELECT `blob` FROM ", tablename, " WHERE filename='\", filename_, \"' INTO DUMPFILE '\", filename_, \"'\" ",
		"		)); ",
		"	END LOOP load_loop; ",
		"	CLOSE cur_export_blob; ",

		"END"
	), false);
	CALL util.exec_outside("CALL temp.export_blob()");
	CALL util.exec("DROP PROCEDURE IF EXISTS temp.export_blob", false);
END;;




