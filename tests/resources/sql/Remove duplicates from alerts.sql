drop table if exists temp_delete_me;
create table temp_delete_me as
Select
	id
from 
	alerts a
join (
	select 
		tdad_id,
		count(1),
		max(id) max_id
	from 
		alerts
	where
		reason = 'alert_exception'
	group by
		tdad_id
	having 
		count(1) > 1
	) b on a.tdad_id=b.tdad_id and a.id <> b.max_id
;

delete from alerts where id in (SELECT id FROM temp_delete_me);	


# create unique index alerts_reason_tdad_id on alerts(tdad_id, reason);
commit;
