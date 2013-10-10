create database hg_repo;

use hg_repo;

drop table if exists changesets;
create table changesets (
  repo            VARCHAR(100),
  `date`          DATETIME,
  node            VARCHAR(40),
  revision        INTEGER,
  author          VARCHAR(300),
  branches        VARCHAR(300),
  file_changes    VARCHAR(300),
  file_adds       VARCHAR(300),
  file_dels       VARCHAR(300),
  parents         VARCHAR(300),
  tags            VARCHAR(300),
  description     VARCHAR(16000)
);
  