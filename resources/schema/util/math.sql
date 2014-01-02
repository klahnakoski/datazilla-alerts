drop database if exists math;
create database math;
use math;
SET SESSION binlog_format = 'ROW';

DELIMITER ;;



DROP FUNCTION IF EXISTS math.min_date;;
CREATE FUNCTION math.min_date (
	a	DATETIME,
	b	DATETIME
)
	RETURNS datetime
 	DETERMINISTIC
	NO SQL
BEGIN
	RETURN
	CASE
	WHEN a IS NULL THEN b
	WHEN b IS NULL THEN a
	WHEN a>b THEN b
	ELSE a
	END;
END;;



DROP FUNCTION IF EXISTS math.maxof;;
CREATE FUNCTION math.maxof (
	a	DECIMAL(20,10),
	b	DECIMAL(20,10)
)
	RETURNS DECIMAL(20,10)
 	DETERMINISTIC
	NO SQL
BEGIN
	RETURN
	CASE
	WHEN a IS NULL THEN b
	WHEN b IS NULL THEN a
	WHEN a>b THEN a
	ELSE b
	END;
END;;




DROP FUNCTION IF EXISTS math.minof;;
CREATE FUNCTION math.minof (
	a	DECIMAL(20,10),
	b	DECIMAL(20,10)
)
	RETURNS DECIMAL(20,10)
 	DETERMINISTIC
	NO SQL
BEGIN
	RETURN
	CASE
	WHEN a IS NULL THEN b
	WHEN b IS NULL THEN a
	WHEN a<b THEN a
	ELSE b
	END;
END;;

DROP FUNCTION IF EXISTS math.mod;;
CREATE FUNCTION math.mod (
	a	INTEGER,
	m	INTEGER
)
	RETURNS INTEGER
 	DETERMINISTIC
	NO SQL
BEGIN
	DECLARE temp INTEGER;

	SET temp=a-floor(a/m)*m;
	return mod(temp, m);
END;;




DROP PROCEDURE IF EXISTS math.do_nothing;;
CREATE PROCEDURE math.do_nothing() BEGIN
END;;


DELIMITER ;



drop table if exists math.digits;
create table math.digits(
	digit  integer
);
insert into math.digits values (0);
insert into math.digits values (1);
insert into math.digits values (2);
insert into math.digits values (3);
insert into math.digits values (4);
insert into math.digits values (5);
insert into math.digits values (6);
insert into math.digits values (7);
insert into math.digits values (8);
insert into math.digits values (9);



DROP FUNCTION IF EXISTS math.bayesian_add;;
CREATE FUNCTION math.bayesian_add(
	a DOUBLE,
	b DOUBLE
)
	RETURNS DOUBLE
DETERMINISTIC
NO SQL
	BEGIN
		RETURN a * b / (a * b + (1 - a) * (1 - b));
	END;;







-- TAKE JSON ARRAY OF NUMBERS AND RETURN ARRAY OF CENTERED STATS
DROP FUNCTION IF EXISTS math.stats;;
CREATE FUNCTION math.stats(
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

SELECT math.stats("[32,56,38,45,30]") FROM dual;;

