create database telepath;

drop table if exists monthly;
create table monthly (
	epoch tinyint unsigned,
	lang varchar(64),
    uri varchar(1024),
    cnt integer unsigned
) engine=myisam;

create index monthly1 ON monthly (epoch,lang,cnt)