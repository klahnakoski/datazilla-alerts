"C:\Program Files\MySQL\MySQL Server 5.5\bin\mysqldump.exe" --no-data  --add-drop-database --extended-insert=FALSE   --skip-dump-date -u root -p{{password}} -h localhost alerts > alerts.sql
