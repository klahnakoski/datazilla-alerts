DELIMITER ;;



DROP PROCEDURE IF EXISTS email_send;;
CREATE PROCEDURE email_send(
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

	SET contentID=util_newid();
	
	START TRANSACTION;

	INSERT INTO email_content (id, subject, date_sent, body) VALUES (
		contentID,
		subject_,
		NULL,
		body_
	);


	IF (attachment_ IS NOT NULL) THEN
		SET attachmentID=util_newid();

		INSERT INTO email_attachment (
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
		SET nextTo=locate(";", remainingTo);
		IF nextTo=0 THEN SET nextTo=length(remainingTo)+1; END IF;
		SET deliveryTo =trim(substring(remainingTo, 1, nextTo-1));
		SET remainingTo=trim(substring(remainingTo, nextTo+1));
		SET deliveryID=util_newid();

		INSERT INTO email_delivery (id, deliver_to, content) VALUES (
			deliveryID,
			deliveryTo,
			contentID
		);
	END LOOP;
	
	UPDATE email_notify SET new_mail=1;

	COMMIT;
END;;






DROP PROCEDURE IF EXISTS `migrate v1.2`;;
CREATE PROCEDURE `migrate v1.2` ()
m11: BEGIN
	DECLARE version VARCHAR(10);

	CALL get_version(version);
	IF (version<>'1.1') THEN
		LEAVE m11;
	END IF;


	DROP TABLE IF EXISTS email_notify;
	DROP TABLE IF EXISTS email_attachment;
	DROP TABLE IF EXISTS email_delivery;
	DROP TABLE IF EXISTS email_content;
	DROP TABLE IF EXISTS email_connection;



	CREATE TABLE email_content (
		id			INTEGER primary key not null,
		subject		VARCHAR(100),
		date_sent	DATETIME,
		body		VARCHAR(16000)
	);

	CREATE TABLE email_delivery (
		id 			INTEGER primary key not null,
		deliver_to	VARCHAR(200),
		content		INTEGER not null,
		CONSTRAINT email_content_fk FOREIGN KEY (content) REFERENCES email_content(id)
	);


	## USE THIS TABLE TO EASY FIND IF THERE ARE NEW MAILS TO SEND
	CREATE TABLE email_notify(
		new_mail	DECIMAL(1)
	);
	INSERT INTO email_notify VALUES (0);
	
	

	CREATE TABLE email_attachment (
		id 			INTEGER primary key not null,
		content		INTEGER not null,
		file		VARCHAR(200),
		CONSTRAINT email_attachment_content_fk FOREIGN KEY (content) REFERENCES email_content(id)
	);


	## USE THIS TABLE TO FIGURE OUT WHICH DAEMON IS THE LATEST TO CONNECT (AND HAS PRIORITY)
	CREATE TABLE email_connection (
		id 	VARCHAR(30) not null
	);
	INSERT INTO email_connection VALUES ("20000101000000");


	CALL email_send(
		"klahnakoski@mozilla.com; klahnakoski@mozilla.com",
		concat("Test", date_format(now(), '%Y%m%d %H%i%s')),
		"This is the content body",
		"./test.txt"
	);



	UPDATE `database` SET version='1.2';

END;;


CALL `migrate v1.2`();;
COMMIT;;




	


