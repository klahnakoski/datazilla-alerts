
USE ekyle_perftest_1;

DELIMITER ;;


DROP PROCEDURE IF EXISTS temp_find_dups;;
CREATE PROCEDURE temp_find_dups(
	START	integer
) 
BEGIN
	declare start integer default 0;
	
	WHILE start<3000000 DO
		select
			test_run_id,
			page_url,
			count(1) num,
			min(id) min_id
		from
			test_data_all_dimensions t
		WHERE
			test_run_id between start and start+100000
		GROUP BY
			test_run_id,
			page_url
		HAVING
			count(1)>1
		;
		SET start=start+100000;
	END WHILE;
END;;
	

CALL temp_find_dups(@start);;
