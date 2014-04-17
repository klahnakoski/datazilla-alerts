drop database if exists time;
create database time;
use time;

commit;
SET SESSION binlog_format = 'ROW';
DELIMITER ;;

DROP FUNCTION IF EXISTS time.isDST;;
CREATE FUNCTION time.isDST(testDate DATETIME)
	RETURNS tinyint(1)
	DETERMINISTIC
BEGIN
		DECLARE daylightSavings 		boolean DEFAULT FALSE;
		DECLARE n_days_to_second_sunday int;
		DECLARE n_days_to_first_sunday	int;
		DECLARE first_of_march			DATETIME;
		DECLARE second_sunday_in_march	DATETIME;
		DECLARE first_of_nov			DATETIME;
		DECLARE first_sunday_in_nov 	DATETIME;
		SET first_of_march			= str_to_date( concat( year(testDate), '-03-01 02:00'), '%Y-%m-%d %H:%i');
		SET first_of_nov			= str_to_date( concat( year(testDate), '-11-01 02:00'), '%Y-%m-%d %H:%i');
		SET n_days_to_second_sunday = mod( 8 - dayofweek(first_of_march), 7) + 7;
		SET second_sunday_in_march	= date_add( first_of_march, INTERVAL n_days_to_second_sunday DAY);
		SET n_days_to_first_sunday	= mod( 8 - dayofweek(first_of_nov), 7);
		SET first_sunday_in_nov 	= date_add( first_of_nov, INTERVAL n_days_to_first_sunday DAY);
		IF ((testDate >= second_sunday_in_march) AND (testDate < first_sunday_in_nov)) THEN
			SET daylightSavings = TRUE;
		ELSE
			SET daylightSavings = FALSE;
		END IF;
		RETURN daylightSavings;
	END
;;


DROP FUNCTION IF EXISTS time.GMT2EDT;;
CREATE FUNCTION time.GMT2EDT(
	gmt 		DATETIME
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	DECLARE output DATETIME;
	SET output=date_add(gmt, INTERVAL -5 HOUR);
	IF (time_isDST(output)) THEN RETURN date_add(output, INTERVAL 1 HOUR); END IF;
	RETURN output;
END;;

DROP FUNCTION IF EXISTS time.GMT2CDT;;
CREATE FUNCTION time.GMT2CDT(
	gmt 		DATETIME
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	DECLARE output DATETIME;
	SET output=date_add(gmt, INTERVAL -6 HOUR);
	IF (time_isDST(output)) THEN RETURN date_add(output, INTERVAL 1 HOUR); END IF;
	RETURN output;
END;;


DROP FUNCTION IF EXISTS time.GMT2MDT;;
CREATE FUNCTION time.GMT2MDT(
	gmt 		DATETIME
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	DECLARE output DATETIME;
	SET output=date_add(gmt, INTERVAL -7 HOUR);
	IF (time_isDST(output)) THEN RETURN date_add(output, INTERVAL 1 HOUR); END IF;
	RETURN output;
END;;


DROP FUNCTION IF EXISTS time.EDT2GMT;;
CREATE FUNCTION time.EDT2GMT(
	edt 		DATETIME
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	IF (time_isDST(edt)) THEN RETURN date_add(edt, INTERVAL 4 HOUR); END IF;
	RETURN date_add(edt, INTERVAL 5 HOUR);
END;;

DROP FUNCTION IF EXISTS time.MDT2GMT;;
CREATE FUNCTION time.MDT2GMT(
	mdt 		DATETIME
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	IF (time_isDST(mdt)) THEN RETURN date_add(mdt, INTERVAL 6 HOUR); END IF;
	RETURN date_add(mdt, INTERVAL 7 HOUR);
END;;

DROP FUNCTION IF EXISTS time.MDT2EDT;;
CREATE FUNCTION time.MDT2EDT(
	mdt 		DATETIME
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	RETURN time.gmt2edt(time_mdt2gmt(mdt));
END;;




DROP FUNCTION IF EXISTS time.convert;;
CREATE FUNCTION time.convert(
	action_time	DATETIME,
	from_zone	VARCHAR(4),
	to_zone		VARCHAR(4)
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	DECLARE output DATETIME;

	#CONVERT TO GMT
	SET output=action_time;
	IF mid(from_zone, 2, 1)='D' AND time.isDST(action_time) THEN
		SET output=date_add(action_time, INTERVAL -1 HOUR);
	END IF;

	IF from_zone='GMT' THEN
		SET output=output;
	ELSEIF left(from_zone, 1)='E' THEN
		SET output=date_add(output, INTERVAL 5 HOUR);
	ELSEIF left(from_zone, 1)='C' THEN
		SET output=date_add(output, INTERVAL 6 HOUR);
	ELSEIF left(from_zone, 1)='M' THEN
		SET output=date_add(output, INTERVAL 7 HOUR);
	ELSEIF left(from_zone, 1)='P' THEN
		SET output=date_add(output, INTERVAL 8 HOUR);
	ELSEIF left(from_zone, 1)='A' THEN
		SET output=date_add(output, INTERVAL 4 HOUR);
	ELSE
		RETURN NULL;
	END IF;

	#CONVERT TO LOCAL
	IF to_zone='GMT' THEN
		SET output=output;
	ELSEIF left(to_zone, 1)='E' THEN
		SET output=date_add(output, INTERVAL -5 HOUR);
	ELSEIF left(to_zone, 1)='C' THEN
		SET output=date_add(output, INTERVAL -6 HOUR);
	ELSEIF left(to_zone, 1)='M' THEN
		SET output=date_add(output, INTERVAL -7 HOUR);
	ELSEIF left(to_zone, 1)='P' THEN
		SET output=date_add(output, INTERVAL -8 HOUR);
	ELSEIF left(to_zone, 1)='A' THEN
		SET output=date_add(output, INTERVAL -4 HOUR);
	ELSE
		RETURN NULL;
	END IF;

	IF (mid(to_zone, 2, 1)='D' AND time.isDST(output)) THEN RETURN date_add(output, INTERVAL 1 HOUR); END IF;
	RETURN output;
END;;





###############################################################################
## GIVEN A DATETIME, AND EOD, RETURN THE POSTED DATE
###############################################################################
DROP FUNCTION IF EXISTS time.eod_post;;
CREATE FUNCTION time.eod_post(
	action_date 		DATETIME,
	action_timezone		VARCHAR(4),
	eod_time			VARCHAR(8), #'%H:%m:%s' FORMAT
	eod_timezone		VARCHAR(4)
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	DECLARE local_time DATETIME;
	DECLARE post_date DATETIME;

	SET local_time=time.convert(action_date, action_timezone, eod_timezone);

	IF date_format(local_time, '%H:%i:%s')<eod_time THEN
		SET post_date=date(local_time);
	ELSE
		SET post_date=date(date_add(local_time, INTERVAL 1 DAY));
	END IF;

	#JAN 1 IS SPECIAL, IT HAS THE EOY NUMBERS
	IF month(post_date)=1 AND day(post_date)=1 THEN RETURN post_date; END IF;

	#POST TO THE FOLLOWING MONDAY IF ON WEEKEND
	IF dayofweek(post_date)=1 THEN RETURN date_add(post_date, INTERVAL 1 DAY); END IF;
	IF dayofweek(post_date)=7 THEN RETURN date_add(post_date, INTERVAL 2 DAY); END IF;
	RETURN post_date;
END;;




DROP FUNCTION IF EXISTS time.cron2tod;;
CREATE FUNCTION time.cron2tod(
	cron_expression		VARCHAR(40)
) RETURNS
	DATETIME
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE output DATETIME;
	DECLARE formatted VARCHAR(30);

	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
		RETURN NULL;
	END;

	RETURN str_to_date(concat('1970-01-01 ',
		LPAD(string.get_word(cron_expression, " ", 2), 2, '0'),':',
		LPAD(string.get_word(cron_expression, " ", 1), 2, '0'),':',
		LPAD(string.get_word(cron_expression, " ", 0), 2, '0')
	), '%Y-%m-%d %H:%i:%s');
END;;



###############################################################################
## ONLY MATCHES DOWN TO THE MINUTE
###############################################################################
DROP FUNCTION IF EXISTS time.cron_match;;
CREATE FUNCTION time.cron_match(
	test_time			DATETIME,
	cron_expression		VARCHAR(40)
) RETURNS
	INTEGER
	NO SQL
	DETERMINISTIC
BEGIN
	DECLARE hourType VARCHAR(30);

	SET hourType=replace(string.get_word(cron_expression, " ", 2), ',0', ',');

	IF hourType='*' THEN
		SET hourType=hourType;
	ELSEIF string.is_numeric(hourType) THEN
		IF hour(test_time) <> CONVERT(hourType, SIGNED INTEGER) THEN
			RETURN 0;
		END IF;
	ELSE #EXPECTING A LIST
		IF
			(hourType REGEXP concat(',', CONVERT(hour(test_time), CHAR(30)), '$'))=0 AND
			(hourType REGEXP concat(',', CONVERT(hour(test_time), CHAR(30)), ','))=0 AND
			(hourType REGEXP concat('^', CONVERT(hour(test_time), CHAR(30)), ','))=0
		THEN
			RETURN 0;
		END IF;
	END IF;

	IF string.is_numeric(string.get_word(cron_expression, " ", 1))=1 THEN
		IF minute(test_time) <> CONVERT(string.get_word(cron_expression, " ", 1), SIGNED INTEGER) THEN
			RETURN 0;
		END IF;
	END IF;

	#IF string.is_numeric(string.get_word(cron_expression, " ", 0)) THEN
	#	IF second(test_time) <> CONVERT(string.get_word(cron_expression, " ", 0), SIGNED INTEGER) THEN
	#		RETURN 0;
	#	END IF;
	#END IF;

	RETURN 1;
END;;



DROP FUNCTION IF EXISTS time.cron2duration;;
CREATE FUNCTION time.cron2duration(
	cron_expression		VARCHAR(40)
) RETURNS
	VARCHAR(30)
	NO SQL
	DETERMINISTIC
BEGIN
	RETURN
	CASE
	WHEN
		string.get_word(cron_expression, " ", 2)="*"
	THEN 'HOUR'
	WHEN
		string.get_word(cron_expression, " ", 3)="*" AND
		string.get_word(cron_expression, " ", 5)="?"
	THEN 'DAY'
	WHEN
		string.get_word(cron_expression, " ", 3)="?" AND
		string.get_word(cron_expression, " ", 5)="*"
	THEN 'DAY'
	WHEN
		string.get_word(cron_expression, " ", 3)="?"
	THEN 'WEEK'
	WHEN
		string.get_word(cron_expression, " ", 5)="?"
	THEN 'MONTH'
	ELSE
		'UNKNOWN'
	END;
END;;

###############################################################################
## RETURN THE DIFFERENCE IN DAYS
###############################################################################
DROP FUNCTION IF EXISTS time.diff;;
CREATE FUNCTION time.diff(
	toTime		DATETIME,
	fromTime	DATETIME
) RETURNS
	DECIMAL(20,10)
	NO SQL
	DETERMINISTIC
BEGIN
	RETURN (unix_timeStamp(toTime)-unix_timestamp(fromTime))/60/60/24;
END;;


#######################################################
## PUT RELATIVE TIME RANGES INTO A TABLE
#######################################################
DROP PROCEDURE IF EXISTS time.ranges;;
CREATE PROCEDURE time.ranges (
	`date` 		DATETIME,
	table_name	VARCHAR(80)
)
BEGIN
	DECLARE BeginOfDay DATETIME;
	DECLARE EndOfDay DATETIME;
	DECLARE m INTEGER;				#LOOP THROUGH MONTHS
	DECLARE w INTEGER;				#LOOP THROUGH WEEKS
	DECLARE d INTEGER;				#LOOP THROUGH DAYS

	IF `date` IS NULL OR trim(`date`)="" THEN
		SET BeginOfDay=date(now());
	ELSE
		SET BeginOfDay=date(`date`);
	END IF;
	SET EndOfDay=date_add(BeginOfDay, INTERVAL 1 DAY);


	DROP TEMPORARY TABLE IF EXISTS time.temp_ranges ;
	CREATE TEMPORARY TABLE time.temp_ranges  (
		ordering INTEGER,
		name 	VARCHAR(30),
		code	VARCHAR(30),
		minDate	DATETIME,
		maxDate	DATETIME
	);
	INSERT INTO time.temp_ranges  VALUES (100, concat('Day Ending ',date_format(BeginOfDay, '%Y-%m-%d')), 'thisDay', BeginOfDay, EndOfDay);
	INSERT INTO time.temp_ranges  VALUES (101, date_format(date_add(BeginOfDay, INTERVAL -1 DAY), '%b-%d'), 'lastDay', date_add(BeginOfDay, INTERVAL -1 DAY), date_add(EndOfDay, INTERVAL -1 DAY));
	INSERT INTO time.temp_ranges  VALUES (200, 'Previous 7 Days', 'this7Day', date_add(EndOfDay, INTERVAL -7 DAY), EndOfDay);


	SET d=2;
	dayy: LOOP BEGIN
		CALL util.exec(concat(
		"	INSERT INTO time.temp_ranges  VALUES (",
		"		100+", d, ", ",
		"		date_format(date_add('",BeginOfDay,"', INTERVAL -",d," DAY), '%b-%d'),",
		"		'lastDay",d,"', ",
		"		date_add('",BeginOfDay,"', INTERVAL -",d," DAY), ",
		"		date_add('",EndOfDay,"', INTERVAL -",d," DAY) ",
		"	)"
		), false);

		SET d=d+1;
		IF d=14 THEN LEAVE dayy; END IF;
	END; END LOOP;




	INSERT INTO time.temp_ranges  VALUES (
		300,
		'This Week',
		'thisWeek',
		date_add(BeginOfDay, INTERVAL -DAYOFWEEK(BeginOfDay)+1 DAY),
		EndOfDay
		);
	INSERT INTO time.temp_ranges  VALUES (
		400,
		'Last full week', #date_format(date_add(date_add(BeginOfDay, INTERVAL -DAYOFWEEK(BeginOfDay)+2 DAY), INTERVAL -1 WEEK), '%M-%d'),
		'lastWeek',
		date_add(date_add(BeginOfDay, INTERVAL -DAYOFWEEK(BeginOfDay)+1 DAY), INTERVAL -1 WEEK),
		         date_add(BeginOfDay, INTERVAL -DAYOFWEEK(BeginOfDay)+1 DAY)
		);

	SET w=2;
	wek: LOOP BEGIN
		CALL util.exec(concat(
		"	INSERT INTO time.temp_ranges  VALUES (",
		"		400+", w, ", ",
		"		date_format(date_add(date_add('",BeginOfDay,"', INTERVAL -DAYOFWEEK('",BeginOfDay,"')+2 DAY), INTERVAL -",w," WEEK), '%b-%d'),",
		"		'lastWeek",w,"', ",
		"		date_add(date_add('",BeginOfDay,"', INTERVAL -DAYOFWEEK('",BeginOfDay,"')+1 DAY), INTERVAL -",w," WEEK), ",
		"		date_add(date_add('",BeginOfDay,"', INTERVAL -DAYOFWEEK('",BeginOfDay,"')+1 DAY), INTERVAL -",w-1," WEEK) ",
		"	)"
		), false);

		SET w=w+1;
		IF w=14 THEN LEAVE wek; END IF;
	END; END LOOP;


	INSERT INTO time.temp_ranges  VALUES (
		402,
		'Last 13 Weeks (91 days)',
		'this13Week',
		date_add(EndOfDay, INTERVAL -91 DAY),
		EndOfDay
		);

	INSERT INTO time.temp_ranges  VALUES (
		402,
		'Last 4 Weeks (28 days)',
		'this4Week',
		date_add(EndOfDay, INTERVAL -28 DAY),
		EndOfDay
		);

	INSERT INTO time.temp_ranges  VALUES (1000, 'Previous 30 Days', 'this30Day', date_add(EndOfDay, INTERVAL -30 DAY), EndOfDay);
	INSERT INTO time.temp_ranges  VALUES (1050, 'Previous 90 Days', 'this90Day', date_add(EndOfDay, INTERVAL -90 DAY), EndOfDay);


	INSERT INTO time.temp_ranges  VALUES (
		1100,
		'Month to Date',
		'thisMonth',
		date_add(BeginOfDay, INTERVAL -DAYOFMONTH(BeginOfDay)+1 DAY),
		EndOfDay
		);
	INSERT INTO time.temp_ranges  VALUES (
		1201,
		concat(left(date_format(date_add(date_add(BeginOfDay, INTERVAL -DAYOFMONTH(BeginOfDay)+1 DAY), INTERVAL -1 MONTH), "%M"), 3), date_format(date_add(date_add(BeginOfDay, INTERVAL -DAYOFMONTH(BeginOfDay)+1 DAY), INTERVAL -1 MONTH), " %Y")),
		'lastMonth',
		date_add(date_add(BeginOfDay, INTERVAL -DAYOFMONTH(BeginOfDay)+1 DAY), INTERVAL -1 MONTH),
		         date_add(BeginOfDay, INTERVAL -DAYOFMONTH(BeginOfDay)+1 DAY)
		);


	SET m=2;
	mon: LOOP BEGIN
		CALL util.exec(concat(
		"	INSERT INTO time.temp_ranges  VALUES (",
		"		1200+", m, ", ",
		"		date_format(date_add(date_add('",BeginOfDay,"', INTERVAL -DAYOFMONTH('",BeginOfDay,"')+1 DAY), INTERVAL -",m," MONTH), '%b %Y'), ",
		"		'lastMonth",m,"',  ",
		"		date_add(date_add('",BeginOfDay,"', INTERVAL -DAYOFMONTH('",BeginOfDay,"')+1 DAY), INTERVAL -",m," MONTH), ",
		"		date_add(date_add('",BeginOfDay,"', INTERVAL -DAYOFMONTH('",BeginOfDay,"')+1 DAY), INTERVAL -",m-1," MONTH) ",
		"	)"
		), false);

		SET m=m+1;
		IF m=14 THEN LEAVE mon; END IF;
	END; END LOOP;

	INSERT INTO time.temp_ranges  VALUES (1300, 'Previous 365 Days', 'this365Day', date_add(EndOfDay, INTERVAL -365 DAY), EndOfDay);

	INSERT INTO time.temp_ranges  VALUES (
		1400,
		'Previous Full Year',
		'lastYear',
		date_add(date_add(BeginOfDay, INTERVAL -DAYOFYEAR(BeginOfDay)+1 DAY), INTERVAL -1 YEAR),
		         date_add(BeginOfDay, INTERVAL -DAYOFYEAR(BeginOfDay)+1 DAY)
		);
	INSERT INTO time.temp_ranges  VALUES (
		1500,
		'Year to Date',
		'thisYear',
		date_add(BeginOfDay, INTERVAL -DAYOFYEAR(BeginOfDay)+1 DAY),
		EndOfDay
		);


	INSERT INTO time.temp_ranges  VALUES (
		1500,
		'For All Time',
		'all',
		str_to_date('19000101', '%Y%m%d'),
		EndOfDay
		);

	CALL util.exec(concat(
		"DROP TABLE IF EXISTS `", table_name, "`"
	), false);

	CALL util.exec(concat(
		"CREATE TABLE ", table_name, " AS SELECT * FROM time.temp_ranges "
	), false);
END;;

## LEVERAGE SQL PARSER TO DEAL WITH INTERVAL SYNTAX
DROP FUNCTION IF EXISTS time.add;;
CREATE FUNCTION time.add(
	value 			DATETIME,
	`interval`		VARCHAR(20)   #DAY HOUR MINUTE SECOND
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	DECLARE output VARCHAR(30);
	DECLARE num DECIMAL(10);
	DECLARE type VARCHAR(30);

	SET output= replace(string.trim(`interval`), "  ", " ");
	IF instr(output, " ")=0 THEN
		SET num=1;
		SET type=string.get_word(output, " ", 1);
	ELSE
		SET num=convert(string.get_word(output, " ", 0), DECIMAL);
		SET type=string.get_word(output, " ", 1);
	END IF;

	IF type='DAY' THEN
		RETURN date_add(value, INTERVAL num DAY);
	ELSEIF type='MONTH' THEN
		RETURN date_add(value, INTERVAL num MONTH);
	ELSEIF type='HOUR' THEN
		RETURN date_add(value, INTERVAL num HOUR);
	ELSEIF type='WEEK' THEN
		RETURN date_add(value, INTERVAL num WEEK);
	ELSEIF type='MINUTE' THEN
		RETURN date_add(value, INTERVAL num MINUTE);
	ELSEIF type='SECOND' THEN
		RETURN date_add(value, INTERVAL num SECOND);
	ELSEIF type='YEAR' THEN
		RETURN date_add(value, INTERVAL num YEAR);
	ELSE
		CALL debug.error(concat(type, ' is not supported'));
	END IF;
	RETURN NULL;
END;;





DROP PROCEDURE IF EXISTS time.range;;
CREATE PROCEDURE time.range(
	tablename	VARCHAR(100),
	`min`		DATETIME,
	`max`		DATETIME,
	`interval`	VARCHAR(10)
) BEGIN
	DECLARE curr DATETIME;

 	CALL util.exec(concat("DROP TABLE IF EXISTS ",tablename), true);
 	CALL util.exec(concat(
 		"CREATE TABLE ",tablename," ( ",
 			"name	VARCHAR(50), ",
 			"value	DATETIME, ",
 			"`min`	DATETIME, ",
 			"`max`	DATETIME  ",
 		")"
 	), false);

	SET curr=`min`;

 	dayy: LOOP
 		CALL util.exec(concat(
 			"INSERT INTO ",tablename,"  VALUES (",
 				cnv.varchar2sql(date_format(curr, "%Y-%m-%d %H:%i:%s")),", ",
 				cnv.datetime2sql(curr),", ",
 				cnv.datetime2sql(curr),", ",
 				"date_add(", cnv.datetime2sql(curr),", INTERVAL ",`interval`, ") ",
 			")"
 		), false);
 		SET curr=time.add(curr, `interval`);
 		IF curr>=`max` THEN LEAVE dayy; END IF;
 	END LOOP;

END;;


DROP FUNCTION IF EXISTS time.floor;;
CREATE FUNCTION time.floor(
	value 			DATETIME,
	`interval`		VARCHAR(20)   #DAY HOUR MINUTE SECOND
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	#SET time.zone = "+0:00";
	IF `interval`='HOUR' THEN
		RETURN from_unixtime(floor(unix_timestamp(value)/3600)*3600);
	ELSEIF `interval`='WEEK' THEN
		RETURN date_add(date(value), interval -dayofweek(date(value))+1 day);
	ELSEIF `interval`='DAY' THEN
		RETURN from_unixtime(floor(unix_timestamp(value)/86400)*86400);
	ELSEIF `interval`='MINUTE' THEN
		RETURN from_unixtime(floor(unix_timestamp(value)/60)*60);
	ELSEIF `interval`='SECOND' THEN
		RETURN from_unixtime(unix_timestamp(value));
	ELSEIF `interval`='MONTH' THEN
		RETURN str_to_date(concat(date_format(value, '%Y%m'), '01'), '%Y%m%d');
	ELSEIF `interval`='YEAR' THEN
		RETURN str_to_date(concat(cast(year(value) as CHAR), '0101'), '%Y%m%d');
	ELSE
		CALL debug.error(concat(`interval`, ' is not supported'));
	END IF;
	RETURN NULL;
END;;

SELECT date_format(time.floor(now(), 'DAY'), '%Y-%M-%d %H:%i:%s.%f');;
SELECT date_format(time.floor(now(), 'HOUR'), '%Y-%M-%d %H:%i:%s.%f');;
SELECT date_format(time.floor(now(), 'MINUTE'), '%Y-%M-%d %H:%i:%s.%f');;
SELECT date_format(time.floor(now(), 'SECOND'), '%Y-%M-%d %H:%i:%s.%f');;
SELECT date_format(time.floor(now(), 'MONTH'), '%Y-%M-%d %H:%i:%s.%f');;


CALL time.ranges(null, 'temp_time_ranges');;





DROP FUNCTION IF EXISTS time.str_to_date;;
CREATE FUNCTION time.str_to_date(
	dateString		VARCHAR(500),
	formatString	VARCHAR(500)
) RETURNS DATETIME DETERMINISTIC NO SQL
BEGIN
	DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;

	RETURN str_to_date(dateString, formatString);
END;;












#######################################################
## SIMPLE TIMEZONE TABLE
#######################################################
DROP TABLE IF EXISTS time.time_zones;;
CREATE TABLE time.time_zones (
	code VARCHAR(3),
	name VARCHAR(30)
);;

INSERT INTO time.time_zones VALUES ('ADT', 'Canada/Atlantic');;
INSERT INTO time.time_zones VALUES ('CDT', 'Canada/Central');;
INSERT INTO time.time_zones VALUES ('EDT', 'Canada/Eastern');;
INSERT INTO time.time_zones VALUES ('EDT', 'Canada/East-Saskatchewan');;
INSERT INTO time.time_zones VALUES ('MDT', 'Canada/Mountain');;
INSERT INTO time.time_zones VALUES ('ADT', 'Canada/Newfoundland');;
INSERT INTO time.time_zones VALUES ('PDT', 'Canada/Pacific');;
INSERT INTO time.time_zones VALUES ('GMT', 'GMT');;



#######################################################
## JUST DAYS IN A MONTH
#######################################################
DROP TABLE IF EXISTS time.days_in_month;
CREATE TABLE time.days_in_month (
	`day`  INTEGER
);;
INSERT INTO time.days_in_month VALUES(1);;
INSERT INTO time.days_in_month VALUES(2);;
INSERT INTO time.days_in_month VALUES(3);;
INSERT INTO time.days_in_month VALUES(4);;
INSERT INTO time.days_in_month VALUES(5);;
INSERT INTO time.days_in_month VALUES(6);;
INSERT INTO time.days_in_month VALUES(7);;
INSERT INTO time.days_in_month VALUES(8);;
INSERT INTO time.days_in_month VALUES(9);;
INSERT INTO time.days_in_month VALUES(10);;
INSERT INTO time.days_in_month VALUES(11);;
INSERT INTO time.days_in_month VALUES(12);;
INSERT INTO time.days_in_month VALUES(13);;
INSERT INTO time.days_in_month VALUES(14);;
INSERT INTO time.days_in_month VALUES(15);;
INSERT INTO time.days_in_month VALUES(16);;
INSERT INTO time.days_in_month VALUES(17);;
INSERT INTO time.days_in_month VALUES(18);;
INSERT INTO time.days_in_month VALUES(19);;
INSERT INTO time.days_in_month VALUES(20);;
INSERT INTO time.days_in_month VALUES(21);;
INSERT INTO time.days_in_month VALUES(22);;
INSERT INTO time.days_in_month VALUES(23);;
INSERT INTO time.days_in_month VALUES(24);;
INSERT INTO time.days_in_month VALUES(25);;
INSERT INTO time.days_in_month VALUES(26);;
INSERT INTO time.days_in_month VALUES(27);;
INSERT INTO time.days_in_month VALUES(28);;
INSERT INTO time.days_in_month VALUES(29);;
INSERT INTO time.days_in_month VALUES(30);;
INSERT INTO time.days_in_month VALUES(31);;



DROP TABLE IF EXISTS time.tod;;
CREATE TABLE time.tod AS
SELECT
	date_add(str_to_date('1970-01-01', '%Y-%m-%d'), INTERVAL (a.digit*10000)+(b.digit*1000)+(c.digit*100)+(d.digit*10)+e.digit MINUTE) value
FROM
	math.digits a,
	math.digits b,
	math.digits c,
	math.digits d,
	math.digits e
WHERE
	a.digit<>9 AND
	(a.digit*10000)+(b.digit*1000)+(c.digit*100)+(d.digit*10)+e.digit<1440
ORDER BY
	value
;;


USE time;;
CALL time.ranges(now(), 'time.temp_time_range_test');;
SELECT * FROM temp_time_range_test;;
