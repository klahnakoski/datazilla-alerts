DROP TABLE IF EXISTS `test_data_all_dimensions`;
CREATE TABLE `test_data_all_dimensions` (
	`id` bigint(20) NOT NULL AUTO_INCREMENT,
	`test_run_id` int(11) NOT NULL,
	`product_id` int(11) NOT NULL,
	`operating_system_id` int(11) NOT NULL,
	`test_id` int(11) NOT NULL,
	`page_id` int(11) NOT NULL,
	`date_received` int(11) unsigned NOT NULL,
	`revision` varchar(16) COLLATE utf8_bin DEFAULT NULL,
	`product` varchar(50) COLLATE utf8_bin NOT NULL,
	`branch` varchar(128) COLLATE utf8_bin NOT NULL,
	`branch_version` varchar(16) COLLATE utf8_bin DEFAULT NULL,
	`operating_system_name` varchar(50) COLLATE utf8_bin NOT NULL,
	`operating_system_version` varchar(50) COLLATE utf8_bin NOT NULL,
	`processor` varchar(25) COLLATE utf8_bin NOT NULL,
	`build_type` varchar(25) COLLATE utf8_bin NOT NULL,
	`machine_name` varchar(255) COLLATE utf8_bin NOT NULL,
	`pushlog_id` int(11) DEFAULT NULL,
	`push_date` int(11) DEFAULT NULL,
	`test_name` varchar(128) COLLATE utf8_bin NOT NULL,
	`page_url` varchar(255) DEFAULT NULL COLLATE utf8_bin,
	`mean` double NOT NULL,
	`std` double,
	`h0_rejected` tinyint(4) DEFAULT 0,
	`p` double DEFAULT NULL,
	`n_replicates` int(11) DEFAULT NULL,
	`fdr` tinyint(4) DEFAULT NULL,
	`trend_mean` double DEFAULT NULL,
	`trend_std` double DEFAULT NULL,
	`test_evaluation` tinyint(4) DEFAULT NULL,
	`status` tinyint(4) DEFAULT '1',
	PRIMARY KEY (`id`),
	UNIQUE KEY `tdad_id` (`id`),
	KEY `test_run_id_key` (`test_run_id`),
	KEY `date_received_key` (`date_received`),
	KEY `product_id_key` (`product_id`),
	KEY `push_date_key` (`push_date`),
	KEY `revision_key` (`revision`),
	KEY `product_key` (`product`),
	KEY `branch_key` (`branch`),
	KEY `branch_version_key` (`branch_version`),
	KEY `processor_key` (`processor`),
	KEY `machine_name_key` (`machine_name`),
	KEY `test_name_key` (`test_name`),
	KEY `page_url_key` (`page_url`),
	KEY `status_key` (`status`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

create UNIQUE INDEX tdad_trid_page on test_data_all_dimensions(test_run_id, page_url);
create INDEX tdad_rev_test on test_data_all_dimensions(revision, test_name);

create index tdad_prod_br_test_url_osver on test_data_all_dimensions(
	`product`,
	`branch`,
	`test_name`,
	`page_url`,
	`operating_system_version`
);



