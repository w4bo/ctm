use ctm;

-- items per transaction
describe ctm.tmp_celltoid__tbl_milan_standard__lmt_10000000__size_400__sup_4__bins_15__ts_notime__bint_2__unitt_3600;
select tid, latitude, longitude, time_bucket * 3600
from ctm.tmp_celltoid__tbl_milan_standard__lmt_10000000__size_400__sup_4__bins_15__ts_notime__bint_2__unitt_3600
limit 10;

-- items per transaction
select tid, count(distinct itemid) as c
from tmp_transactiontable__tbl_milan_standard__lmt_10000000__size_400__sup_5__bins_15__ts_daily__bint_6__unitt_3600
group by tid
order by c desc;

-- items per transaction in real dataset
select round(round(latitude / (11 * 15), 4) * 11 * 15, 4), round(round(latitude / (15 * 15), 4) * 15 * 15, 4), count(distinct userid, trajectoryid) as c
from trajectory.milan_standard
group by round(round(latitude / (11 * 15), 4) * 11 * 15, 4), round(round(latitude / (15 * 15), 4) * 15 * 15, 4)
order by c desc;

-- item per transaction
select distinct tid, neigh, space_distance
from tmp_neighborhood__tbl_milan_standard__lmt_10000000__size_400__sup_5__bins_15__ts_daily__bint_6__unitt_3600
where tid = 50
order by tid asc, neigh asc;

select count(*) from tmp_neighborhood__tbl_milan_standard__lmt_10000000__size_400__sup_5__bins_15__ts_daily__bint_6__unitt_3600; -- 46K
select count(*) from trajectory.milan_standard; -- 225M
select count(distinct userid, trajectoryid) from trajectory.milan_standard;

-- checking spatial neighbors
select tid, count(distinct l3, l4) 
from tmp_neighborhood__tbl_milan_standard__lmt_10000000__size_400__sup_5__bins_15__ts_daily__bint_6__unitt_3600
where space_distance <= 3 * 123 * 15 and space_distance > 0
group by tid;

-- get spatial neighbors
select distinct l3, l4, space_distance
from tmp_neighborhood__tbl_milan_standard__lmt_10000000__size_400__sup_5__bins_15__ts_daily__bint_6__unitt_3600
where space_distance <= 3 * 123 * 15 and space_distance > 0 and tid = 30;

use trajectory;

-- CREATE TABLE besttrj_standard
-- drop table besttrj_standard;
create table besttrj_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double, accuracy int)
comment 'Filtered trajectories from milan. sql: select customid as userid, timest as timestamp, latitude, longitude, accuracy, trajid as trajectoryid from trajectoryflow_besttrj'
stored as parquet;
insert into besttrj_standard select customid as userid, trajid as trajectoryid, timest as `timestamp`, latitude, longitude, accuracy from trajectoryflow_besttrj;

-- CREATE TABLE geolife_standard
-- drop table geolife_standard;
create table geolife_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double)
comment 'Geolife with standard schema. sql: select customid as userid, timest as timestamp, latitude, longitude, accuracy, trajid as trajectoryid from geolife_standard'
stored as parquet;
insert into geolife_standard select customid as userid, trajid as trajectoryid, unix_timestamp(concat(`date`, ' ', timest), 'yyyy-MM-dd hh:mm:ss') as `timestamp`, latitude, longitude from geolife_bejin;
select count(*) from geolife_standard; -- 18 891 115
select count(distinct userid, trajectoryid) from trajectory.geolife_standard; -- 17 158

create table geolife2_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double)
comment 'Geolife with standard schema, but trajectories id are not kept.'
stored as parquet;
insert into geolife2_standard select customid as userid, customid as trajectoryid, unix_timestamp(concat(`date`, ' ', timest), 'yyyy-MM-dd hh:mm:ss') as `timestamp`, latitude, longitude from geolife_bejin;
select count(*) from geolife2_standard; -- 18 891 115
select count(distinct userid, trajectoryid) from trajectory.geolife2_standard; -- 179

-- CREATE TABLE cariploenr_standard
drop table cariploenr_standard;
create table cariploenr_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double, accuracy int)
comment 'Cariploenr6 with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets. sql: select customid as userid, timest as timestamp, latitude, longitude, accuracy, trajid as trajectoryid from cariploenr6'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
-- partitioned by (userid string)
insert into cariploenr_standard select customid as userid, trajid as trajectoryid, timest as `timestamp`, latitude, longitude, accuracy from cariploenr6;

-- CREATE TABLE oldenburg_standard
create table oldenburg_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double, accuracy int)
comment 'Oldenburg with 1M trajectories.'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
insert into oldenburg_standard select customid as userid, trajid as trajectoryid, timest as `timestamp`, latitude, longitude, accuracy from cariploenr6;

select trajid, latitude, longitude, `timestamp` from final_oldenburg_dataset where customid = 666 or customid = 667 order by trajid, `timestamp` limit 10000;
select customid, trajid, count(trajid) from final_oldenburg_dataset group by customid, trajid;

describe final_oldenburg_dataset;

-- CREATE TABLE milano_standard
drop table milan_standard;
create table milan_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double)
comment 'milan_standard with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets. sql: select userid, trajectoryid, latitude, longitude, `timestamp` from cariploenr_standard where latitude >= 45.4 and latitude <= 45.5 and longitude >= 9.04 and longitude <= 9.275'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
insert into milan_standard select userid, trajectoryid, `timestamp`, latitude, longitude from cariploenr_standard where latitude >= 45.4 and latitude <= 45.5 and longitude >= 9.04 and longitude <= 9.275;
select count(distinct userid, trajectoryid) from trajectory.milan_standard; -- 10 249 665

drop table milan2_standard;
create table milan2_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double)
comment 'milan_standard with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets, trajectory id are not kept. latitude >= 45.4 and latitude <= 45.5 and longitude >= 9.04 and longitude <= 9.275'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
insert into milan2_standard select userid, userid as trajectoryid, `timestamp`, latitude, longitude from cariploenr_standard where latitude >= 45.4 and latitude <= 45.5 and longitude >= 9.04 and longitude <= 9.275;
select count(distinct userid, trajectoryid) from trajectory.milan2_standard; -- 382 191
select min(`timestamp`) from trajectory.milan2_standard; -- 	1504226845

drop table milan2_standard_first7days;
create table milan2_standard_first7days(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double)
comment 'milan_standard with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets, trajectory id are not kept. latitude >= 45.4 and latitude <= 45.5 and longitude >= 9.04 and longitude <= 9.275'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
insert into milan2_standard_first7days select userid, trajectoryid, `timestamp`, latitude, longitude from trajectory.milan2_standard where `timestamp` < unix_timestamp('2017-09-08 00:00:00', 'yyyy-MM-dd hh:mm:ss');
select count(distinct userid, trajectoryid) from trajectory.milan2_standard_first7days; -- 2 026

drop table milan2_standard_first14days;
create table milan2_standard_first14days(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double)
comment 'milan_standard with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets, trajectory id are not kept. latitude >= 45.4 and latitude <= 45.5 and longitude >= 9.04 and longitude <= 9.275'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
insert into milan2_standard_first14days select userid, trajectoryid, `timestamp`, latitude, longitude from trajectory.milan2_standard where `timestamp` < unix_timestamp('2017-09-15 00:00:00', 'yyyy-MM-dd hh:mm:ss');
select count(distinct userid, trajectoryid) from trajectory.milan2_standard_first14days; -- 36 938

drop table milan2_standard_first21days;
create table milan2_standard_first21days(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double)
comment 'milan_standard with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets, trajectory id are not kept. latitude >= 45.4 and latitude <= 45.5 and longitude >= 9.04 and longitude <= 9.275'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
insert into milan2_standard_first21days select userid, trajectoryid, `timestamp`, latitude, longitude from trajectory.milan2_standard where `timestamp` < unix_timestamp('2017-09-022 00:00:00', 'yyyy-MM-dd hh:mm:ss');
select count(distinct userid, trajectoryid) from trajectory.milan2_standard_first21days; -- 152 262

-- CREATE TABLE tdrive_standard
drop table tdrive_standard;
create table tdrive_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double)
comment 'tdriveext with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets. sql: tdrive_standard select customid, customid, unix_timestamp(timest,yyyy-MM-dd hh:mm:ss) as `timestamp`, latitude, longitude from tdriveext where latitude >= 39.6 and latitude <= 40.2 and longitude >= 116.1 and longitude <= 116.7'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
insert into tdrive_standard select customid, customid, unix_timestamp(timest,'yyyy-MM-dd hh:mm:ss') as `timestamp`, latitude, longitude from tdriveext where latitude >= 39.6 and latitude <= 40.2 and longitude >= 116.1 and longitude <= 116.7;

create table tdrive_standard_first24 as select * from tdrive_standard where `timestamp` < 1201996800;
create table tdrive_standard_firstmonth as select * from tdrive_standard where `timestamp` < 1204329600;

select count(*) from cariploenr_standard;
select count(*) from milan_standard;
select count(*) from tdrive_standard;
select count(*) from oldenburg_standard;
select count(*) from geolife_standard;

select
    distinct
    userid, trajectoryid,
    cast(round(round(latitude / 88, 4) * 88, 4) * 10000 as int) as latitude, 
    cast(round(round(longitude / 120, 4) * 120, 4) * 10000 as int) as longitude,
    CAST(`timestamp` / 1 as BIGINT) as bucket_unix_timestamp
from trajectory.milan_standard;



use ctm;
select itemsetid, itemid from ctm.CTM__tbl_tdrive_standard_first24__lmt_10000000__size_50__sup_5__bins_10__ts_absolute__bint_1__unitt_3600__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__itemset;
select itemsetid, tileid from ctm.CTM__tbl_tdrive_standard_first24__lmt_10000000__size_50__sup_5__bins_10__ts_absolute__bint_1__unitt_3600__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__support;
select * from ctm.tmp_celltoid__tbl_tdrive_standard_first24__lmt_10000000__size_50__sup_5__bins_10__ts_absolute__bint_1__unitt_3600__epss_Infinity__epst_Infinity__freq_1__sthr_1000000;
select * from ctm.tmp_transactiontable__tbl_tdrive_standard_first24__lmt_10000000__size_50__sup_5__bins_10__ts_absolute__bint_1__unitt_3600;
describe ctm.tmp_transactiontable__tbl_tdrive_standard_first24__lmt_10000000__size_50__sup_5__bins_10__ts_absolute__bint_1__unitt_3600;

drop table tmp_result_ctm purge;
create table tmp_result_ctm as
    select i.itemsetid, t.itemid, t.tid, s.userid, s.trajectoryid, s.`timestamp`, s.latitude, s.longitude, t.latitude as bin_latitude, t.longitude as bin_longitude, t.time_bucket * 3600 as bin_timestamp, u.tileid as in_support
    from         ctm.tmp_transactiontable__tbl_tdrive_standard_first24__lmt_10000000__size_10__sup_5__bins_10__ts_absolute__bint_1__unitt_3600 t 
            join ctm.CTM__tbl_tdrive_standard_first24__lmt_10000000__size_10__sup_5__bins_10__ts_absolute__bint_1__unitt_3600__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__itemset i on (t.itemid = i.itemid)
            join trajectory.tdrive_standard_first24 s on (t.userid = s.userid 
                and t.trajectoryid = s.trajectoryid and cast(`timestamp` / 3600 as int) = t.time_bucket
                and round(round(s.latitude  / (11 * 10), 4) * (11 * 10), 4) = t.latitude
                and round(round(s.longitude / (15 * 10), 4) * (15 * 10), 4) = t.longitude)
            left join ctm.CTM__tbl_tdrive_standard_first24__lmt_10000000__size_10__sup_5__bins_10__ts_absolute__bint_1__unitt_3600__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__support u on (t.tid = u.tileid);

select cast(`timestamp` / 3600 as int) from trajectory.tdrive_standard_first24 limit 100;
select * from tmp_result_ctm;
select count(*) from trajectory.tmp_result_ctm;

select count(distinct time_bucket, latitude, longitude) from ctm.tmp_transactiontable__tbl_tdrive_standard_first24__lmt_10000000__size_5__sup_20__bins_10__ts_absolute__bint_1__unitt_3600;
DROP DATABASE ctm CASCADE;
create database ctm;