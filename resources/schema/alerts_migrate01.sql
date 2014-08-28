use alerts;

ALTER TABLE alerts 
	ADD COLUMN branch VARCHAR(128) NOT NULL DEFAULT ' ',
	ADD COLUMN test VARCHAR(128) NOT NULL DEFAULT ' ',
	ADD COLUMN platform	varchar(64) NOT NULL DEFAULT ' ',
	ADD COLUMN percent	varchar(32) NOT NULL DEFAULT ' ',
	ADD COLUMN graphurl	varchar(64) NOT NULL DEFAULT ' ',
	ADD COLUMN changeset	varchar(128) NOT NULL DEFAULT ' ',
	ADD COLUMN keyrevision	varchar(128) NOT NULL DEFAULT ' ',
	ADD COLUMN bug	varchar(128),
	ADD COLUMN changesets	varchar(8192),
	ADD COLUMN mergedfrom	varchar(128),
	ADD COLUMN duplicate	varchar(128),
	ADD COLUMN tbplurl	varchar(256)
;

# NOT USED, BUT IN CODE ANYWAYS
ALTER TABLE alerts 
	ADD COLUMN bugcount	int(11) NOT NULL DEFAULT 0,
	ADD COLUMN email	varchar(128),
;
ALTER TABLE alerts 
	ADD COLUMN body BLOB
;


ALTER TABLE alerts CHANGE create_time date DATETIME;
ALTER TABLE alerts MODIFY status VARCHAR(64) NOT NULL DEFAULT 'NEW';
ALTER TABLE stati MODIFY code varchar(64);


UPDATE alerts SET 
	branch=json.string(details, 'Branch'),
	test=coalesce(json.string(json.json(details, 'Test'), 'name'), json.string(details, 'Test')),
	platform=coalesce(json.string(json.json(details,'OS'), 'version'), json.string(details, 'Device')),
	percent=concat(cast(round(json.number(details, 'diff_percent')*100, 1) AS CHAR), '%'),
	keyrevision=revision,
	mergedfrom=''
;
COMMIT;

UPDATE alerts SET mergedfrom='' WHERE mergedfrom IS NULL;

INSERT INTO stati (code) VALUES 
('False Alarm'),
('Investigating'),
('Resolved'),
('Duplicate'),
('Not Tracking'),
('Wont Fix'),
('Ignore'),
('Backout'),
('Too Low'),
('Infra'),
('sqd'),
('sfs')
;

INSERT INTO stati (code) VALUES ('NEW_');
UPDATE alerts SET status='NEW_' WHERE status like 'new';
DELETE FROM stati WHERE code like 'new';
INSERT INTO stati (code) VALUES ('NEW');
UPDATE alerts SET status='NEW' WHERE status='NEW_';
DELETE FROM stati WHERE code='NEW_';

COMMIT;