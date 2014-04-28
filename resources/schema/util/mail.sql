DELIMITER ;;

drop database if exists mail;;
create database mail;;
use mail;;
SET SESSION binlog_format = 'ROW';;


DROP TABLE IF EXISTS mail.notify;;
DROP TABLE IF EXISTS mail.attachment;;
DROP TABLE IF EXISTS mail.delivery;;
DROP TABLE IF EXISTS mail.content;;
DROP TABLE IF EXISTS mail.connection;;



CREATE TABLE mail.content (
	id			INTEGER primary key not null,
	subject		VARCHAR(200),
	date_sent	DATETIME,
	body		MEDIUMTEXT
);;

CREATE TABLE mail.delivery (
	id 			INTEGER primary key not null,
	deliver_to	VARCHAR(200),
	content		INTEGER not null,
	CONSTRAINT mail_content_fk FOREIGN KEY (content) REFERENCES mail.content(id)
);;


CREATE TABLE mail.notify(
	new_mail	DECIMAL(1)
);;

CREATE TABLE mail.attachment (
	id 			INTEGER primary key not null,
	content		INTEGER not null,
	file		VARCHAR(200),
	CONSTRAINT mail_attachment_content_fk FOREIGN KEY (content) REFERENCES mail.content(id)
);;


CREATE TABLE mail.connection (
	id 	VARCHAR(30) not null
);;
INSERT INTO mail.connection VALUES ("20000101000000");;


DROP PROCEDURE IF EXISTS mail.send;;
CREATE PROCEDURE mail.send(
	to_			VARCHAR(8000),##SEMICOLON SEPARATED EMAIL ADDRESSES
	subject_	VARCHAR(200),
	body_		VARCHAR(16000),
	attachment_	VARCHAR(200)
)
es: BEGIN
	DECLARE contentID INTEGER;
	DECLARE deliveryID INTEGER;
	DECLARE attachmentID INTEGER;
	DECLARE remainingTo VARCHAR(8000);
	DECLARE nextTo INTEGER;
	DECLARE deliveryTo VARCHAR(200);

	SET remainingTo=trim(to_);
	IF (length(remainingTo)=0) THEN LEAVE es; END IF;

	SET contentID=util.newid();

	START TRANSACTION;

	INSERT INTO mail.content (id, subject, date_sent, body) VALUES (
		contentID,
		subject_,
		NULL,
		body_
	);


	IF (attachment_ IS NOT NULL) THEN
		SET attachmentID=util.newid();

		INSERT INTO mail.attachment (
			id,
			content,
			file
		) VALUES (
			attachmentID,
			contentID,
			attachment_
		);
	END IF;


	##SEMICOLON SEPARATED EMAIL ADDRESSES
	tos: LOOP
		IF (length(remainingTo)=0) THEN LEAVE tos; END IF;
		SET nextTo=locate(';', remainingTo);
		IF nextTo=0 THEN SET nextTo=length(remainingTo)+1; END IF;
		SET deliveryTo =trim(substring(remainingTo, 1, nextTo-1));
		SET remainingTo=trim(substring(remainingTo, nextTo+1));
		SET deliveryID=util.newID();

		INSERT INTO mail.delivery (id, deliver_to, content) VALUES (
			deliveryID,
			deliveryTo,
			contentID
		);
	END LOOP;

	UPDATE mail.notify SET new_mail=1;

	COMMIT;
END;;


CALL mail.send(
	"kyle@lahnakoski.com; kyle@lahnakoski.com",
	concat("Test", date_format(now(), '%Y%m%d %H%i%s')),
	"This is the content body",
	"./test.txt"
);;







