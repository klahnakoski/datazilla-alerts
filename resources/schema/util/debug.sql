drop database if exists debug;
create database debug;
use debug;
SET SESSION binlog_format = 'ROW';

DELIMITER ;;


DROP PROCEDURE IF EXISTS debug.error;;
CREATE PROCEDURE debug.error (
	message		VARCHAR(1000)
)
BEGIN
	CALL debug.log(message);
	#DECLARE EXIT HANDLER FOR SQLSTATE '42000'
    CALL please_look_at_debug_log_table_for_error_messages;
    ##INSERT INTO this_table_does_not_exist SELECT message;
END;;



DROP PROCEDURE IF EXISTS debug.log;;
CREATE PROCEDURE debug.log (
	message		VARCHAR(8000)
)
BEGIN
	INSERT INTO debug.log_table (action_time, message) VALUES (now(), left(message, 4000));
END;;



CREATE TABLE IF NOT EXISTS debug.log_table (
	action_time		DATETIME,
	message 		VARCHAR(4000)
);

