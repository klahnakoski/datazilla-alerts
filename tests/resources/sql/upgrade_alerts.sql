alter table alerts add (revision  varchar(20));

update alerts set revision=json.string(details, "revision");