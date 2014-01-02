create user 'readall'@'127.0.0.1' identified by 'password';
grant select on *.* to 'readall'@'%';
grant execute on *.* to 'readall'@'%';
