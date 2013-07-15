


use ekyle_perftest_1;
DROP index tdad ON test_data_all_dimensions;
#TOO SELECTIVE
#CREATE UNIQUE INDEX tdad ON test_data_all_dimensions(test_run_id, page_id);
CREATE INDEX tdad_test_run_id2 ON test_data_all_dimensions(test_run_id);

USE pushlog_hgmozilla_1;
ALTER TABLE changesets DROP COLUMN SHORT_revision;
ALTER TABLE changesets ADD COLUMN revision VARCHAR(12);
CREATE INDEX ch_revision ON changesets(revision);





#IT WOULD BE NICE FOR THE APP TO DO THIS, BUT NOT REQUIRED
USE pushlog_hgmozilla_1;
UPDATE changesets SET revision=substring(node, 1, 12) WHERE revision<>substring(node, 1, 12);


use ekyle_perftest_1;
DELETE FROM test_data_all_dimensions;

INSERT INTO `test_data_all_dimensions` ( 
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
	`h0_rejected`, 
	`p`, 
	`n_replicates`, 
	`fdr`, 
	`trend_mean`, 
	`trend_std`, 
	`test_evaluation`
)
SELECT STRAIGHT_JOIN 
	tr.id `test_run_id`, 
	b.product_id `product_id`, 
	o.id `operating_system_id`, 
	tr.test_id `test_id`, 
	tpm.page_id `page_id`, 
	pl.date `date_received`, 
	tr.revision `revision`, 
	p.product `product`, 
	p.branch `branch`, 
	p.version `branch_version`, 
	o.name `operating_system_name`, 
	o.version `operating_system_version`, 
	b.processor `processor`, 
	b.build_type `build_type`,
	m.name `machine_name`, 
	pl.id `pushlog_id`, 
	pl.date `push_date`, 
	t.name `test_name`, 
	pg.url `page_url`, 
	max(CASE WHEN mv.name='mean' THEN tpm.value ELSE 0 END) `mean`, 
	max(CASE WHEN mv.name='stddev' THEN tpm.value ELSE 0 END) `std`, 
	min(CASE WHEN mv.name='h0_rejected' THEN tpm.value ELSE NULL END) `h0_rejected`, 
	min(CASE WHEN mv.name='p' THEN tpm.value ELSE NULL END) `p`, 
	min(CASE WHEN mv.name='n_replicates' THEN tpm.value ELSE NULL END) `n_replicates`, 
	min(CASE WHEN mv.name='fdr' THEN tpm.value ELSE NULL END) `fdr`, 
	min(CASE WHEN mv.name='trend_mean' THEN tpm.value ELSE NULL END) `trend_mean`, 
	min(CASE WHEN mv.name='trend_stddev' THEN tpm.value ELSE NULL END) `trend_std`, 
	min(CASE WHEN mv.name='`test_evaluation' THEN tpm.value ELSE NULL END) `test_evaluation`
FROM 
	`test_run` AS tr 
LEFT JOIN
	test_data_all_dimensions AS tdad ON tdad.test_run_id=tr.id #AND tdad.page_id=tpm.page_id
LEFT JOIN
	`test_page_metric` AS tpm ON tpm.test_run_id = tr.id 
LEFT JOIN 
	`pages` AS pg ON tpm.page_id = pg.id 
LEFT JOIN 
	`build` AS b ON tr.build_id = b.id 
LEFT JOIN 
	`product` AS p ON b.product_id = p.id 
LEFT JOIN 
	`test` AS t ON tr.test_id = t.id 
LEFT JOIN 
	`metric_value` AS mv ON tpm.metric_value_id = mv.id 
LEFT JOIN 
	`machine` AS m ON tr.machine_id = m.id 
LEFT JOIN 
	`operating_system` AS o ON m.operating_system_id = o.id 
LEFT JOIN 
	pushlog_hgmozilla_1.changesets AS ch ON ch.revision=tr.revision
LEFT JOIN
	pushlog_hgmozilla_1.pushlogs AS pl ON pl.id = ch.pushlog_id 
LEFT JOIN 
	pushlog_hgmozilla_1.branches AS br ON pl.branch_id = br.id 
LEFT JOIN 
	pushlog_hgmozilla_1.branch_map AS bm ON br.name = bm.name 
WHERE 
 	tdad.test_run_id IS NULL 
 	AND tpm.page_id IS NOT NULL
 	AND tpm.value IS NOT NULL
--  	AND tr.id>(SELECT max(id)-1000 FROM test_run)  # ONLY THE RECENT test_runs
GROUP BY
	tr.id
;


select count(1) from test_data_all_dimensions;