select 
	* 
from 
	mail.content 
where
	subject = '[ALERT][Mozilla-Inbound] json-parse-financial regressed by 5.2% in kraken'
order by 
	coalesce(date_sent, now()) desc 
limit 
	100