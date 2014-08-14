select 
	* 
from 
	mail.content 
where
	date_sent is null
order by 
	coalesce(date_sent, now()) desc 
limit 
	100