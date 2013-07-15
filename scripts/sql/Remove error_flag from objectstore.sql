select
	error_flag,
	count(1) 
from 
	ekyle_objectstore_1.objectstore
group by
	error_flag
;

update ekyle_objectstore_1.objectstore set error_flag='N' where error_flag='Y';
