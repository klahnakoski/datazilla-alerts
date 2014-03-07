use ekyle_objectstore_1;


SELECT
    id,
    test_run_id,
    date_loaded,
    processed_exception,
--    error_flag,
--    error_msg,
 	substring(json_blob, 1, 8000) json,
-- 	substring(json_blob, locate("revision\":", json_blob), 100) rev,
-- 	string.between( substring(json_blob, 1, 60000), "yelp.com", "]", 1),
    string.between( substring(json_blob, locate("revision\":", json_blob), 100), "revision\": \"", "\",", 1) revision
--    worker_id
FROM
    objectstore
WHERE
	test_run_id=3577944
#	instr(json_blob, "tp5o")>0
# 	instr(lower(json_blob), "693b4eafd936")>0
#    instr(lower(json_blob), "897654df47b6")>0 #AND
# 	and id between 78851 and 131926
#	AND instr(json_blob, "tp5o")>0
ORDER BY
    id
LIMIT
	100
