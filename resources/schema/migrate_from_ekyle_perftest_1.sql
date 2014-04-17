use alerts;

DELETE FROM alerts.reasons;
INSERT INTO alerts.reasons (
	code      ,
	description,
	last_run    ,
	config       ,
	email_subject ,
	email_template 
) 
SELECT
	code      ,
	description,
	last_run    ,
	config       ,
	null,
	email_template 
FROM 
	ekyle_perftest_1.alert_reasons
;

DELETE FROM alerts.listeners;
INSERT INTO alerts.listeners SELECT * FROM ekyle_perftest_1.alert_listeners;


DELETE FROM alerts.alerts;
INSERT INTO alerts.alerts (
id,
status	,
create_time	,
last_updated,
last_sent,
tdad_id,
reason,
details,
severity,
confidence,
solution,
revision
)
SELECT 
id,
status	,
create_time	,
last_updated,
last_sent,
tdad_id,
reason,
details,
severity,
confidence,
solution,
revision
FROM ekyle_perftest_1.alerts;


DELETE FROM alerts.hierarchy;
INSERT INTO alerts.hierarchy(parent, child) SELECT parent, child FROM ekyle_perftest_1.alert_hierarchy;


