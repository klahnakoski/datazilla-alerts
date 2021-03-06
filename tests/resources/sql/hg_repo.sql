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
  children        VARCHAR(1000),
  tags            VARCHAR(600),
  description     VARCHAR(16000)
);


create index changesets_repo_revision on changesets(repo, revision);
create index changesets_repo_node on changesets(repo, node);


alter table changesets modify tags varchar(600)
