use ekyle_perftest_1;
	

select
	from_unixtime(arcavia_string.between(details, 'sh_date":', ',', 1)) push_date,
	a.confidence,
	a.*
from
	alerts a
	
order by
	from_unixtime(arcavia_string.between(details, 'sh_date":', ',', 1)) DESC