
use ekyle_perftest_1;

select 
	a.revision,
	a.create_time,  #aka push_date
	json.number(details, 'confidence') confidence,
	t.product,
	concat(t.branch, ' ', t.branch_version) branch,
	concat(t.operating_system_name, ' ', t.operating_system_version, ' ', t.processor) os,
	t.test_name, 
	t.page_url, 
	json.number(json.json(details, 'past_stats'), 'mean') past_mean,
	json.number(json.json(details, 'future_stats'), 'mean') future_mean,
	round((json.number(json.json(details, 'future_stats'), 'mean') - json.number(json.json(details, 'past_stats'), 'mean'))/json.number(json.json(details, 'past_stats'), 'mean')*100, 2) diff,
	t.mean  #actually is median
from 
	ekyle_perftest_1.alerts a 
left join
	ekyle_perftest_1.test_data_all_dimensions t on t.id=a.tdad_id
WHERE
	reason='alert_sustained' and
 	t.branch not in ('Mozilla-Esr17') and
	t.test_name not in ('tp5n')
ORDER BY
	a.create_time desc
