use trajectory;

drop table hermopoulis_standard_1000;
create table hermopoulis_standard_1000 as
select *
from hermopoulis_standard
where concat_ws('-', userid, trajectoryid) in
      (select concat_ws('-', userid, trajectoryid) from (select userid, trajectoryid, count(*) as c from hermopoulis_standard group by userid, trajectoryid limit 1000) a); --  order by c desc

describe hermopoulis_standard_1000;

select avg(c)
from (
         select concat_ws('-', userid, trajectoryid), count(distinct tid) c from  ctm.tmp_transactiontable__tbl_hermopoulis_standard_1000__lmt_1000000__size_10__sup_1__bins_19__ts_notime__bint_1 group by concat_ws('-', userid, trajectoryid)
     ) a;

select count(distinct tid) from ctm.tmp_transactiontable__tbl_hermopoulis_standard_1000__lmt_1000000__size_10__sup_1__bins_19__ts_notime__bint_1__semf_mobilityplaceactivity;
select * from ctm.tmp_celltoid__tbl_hermopoulis_standard_1000__lmt_1000000__size_10__sup_1__bins_19__ts_notime__bint_1__semf_mobilityplaceactivity;

use trajectory;
create external table if not exists hermopoulis_ext (
    userid string,
    longitude double,
    latitude double,
    `timestamp` double,
    episodesems string,
    dataset int,
    sems0 string,
    sems1 string,
    sems2 string,
    sems3 string,
    sems4 string)
ROW FORMAT DELIMITED
fields terminated by ','
stored as textfile
location '/user/mfrancia/hermopoulis';

select * from hermopoulis_ext limit 10;
select count(*) from hermopoulis_ext; -- 50M
select count(*) from (select distinct userid, dataset from hermopoulis_ext) a; -- 4200
select count(*) from (select distinct userid from hermopoulis_ext) a; -- 1000
select concat(userid, '-', dataset) as userid, latitude, longitude, unix_timestamp(sems1) as `timestamp`, sems2, sems3, sems4 from hermopoulis_ext limit 10;
select distinct sems2 from hermopoulis_ext;
-- 1 MOVE
-- 2 STOP
select distinct sems3 from hermopoulis_ext;
-- 1 UNIVERSITY
-- 2 NIGHTCLUB
-- 3 GATE
-- 4 LEVEL_CROSSING
-- 5 PLACE_OF_WORSHIP
-- 6 DRIVE
-- 7 BANK
-- 8 KINDERGARTEN
-- 9 BUS_STOP
-- 10 BICYCLE
-- 11 DRIVING_SCHOOL
-- 12 CINEMA
-- 13 PUBLIC_BUILDING
-- 14 SUBWAY_ENTRANCE
-- 15 FERRY_TERMINAL
-- 16 FAST_FOOD
-- 17 WALKING
-- 18 CROSSING
-- 19 RESTAURANT
-- 20 BUS
-- 21 CAFE

select distinct sems4 from hermopoulis_ext;
-- 1  WORKING
-- 2  RELAXING
-- 3  SOCIALIZING
-- 4  WITHDRAWING
-- 5  ADMINISTRATION
-- 6  STUDYING
-- 7  EATING
-- 8  SPORTING
-- 9  CHURCHING
-- 10 TRANSPORTATION
-- 11 SHOPPING

select min(latitude), percentile(latitude, 0.5), max(latitude) - min(latitude), min(longitude), max(longitude), max(longitude) - min(longitude) from hermopoulis_ext;
-- 4189774.4255	4217556.209	466594.3918	488905.4412

select min(`timestamp`), max(`timestamp`) from hermopoulis_standard;
-- 1384038000 1384630694
-- Saturday 9 November 2013 23:00:00
-- Saturday 16 November 2013 19:38:14

drop table hermopoulis_standard;
create table hermopoulis_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double, mobility string, place string, activity string)
    comment 'hermopoulis_standard with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets. sql: select userid, userid as trajectoryid, `timestamp`, latitude, longitude, sems2, sems3, sems4 from hermopoulis_ext'
    clustered by(userid) sorted by (`timestamp`) into 200 buckets
    stored as parquet;
insert into hermopoulis_standard select concat_ws('-', userid, cast(dataset as string)), concat_ws('-', userid, cast(dataset as string)), unix_timestamp(sems1), latitude, longitude, sems2, sems3, sems4 from hermopoulis_ext;

drop table hermopoulis_standard1;
create table hermopoulis_standard1(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double, mobility string, place string, activity string)
    comment 'hermopoulis_standard with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets. sql: select concat_ws(-, userid, cast(dataset as string)), concat_ws(-, userid, cast(dataset as string)), unix_timestamp(sems1), latitude, longitude, sems2, sems3, sems4 from hermopoulis_ext where dataset = 1'
    clustered by(userid) sorted by (`timestamp`) into 200 buckets
    stored as parquet;
insert into hermopoulis_standard1 select concat_ws('-', userid, cast(dataset as string)), concat_ws('-', userid, cast(dataset as string)), unix_timestamp(sems1), latitude, longitude, sems2, sems3, sems4 from hermopoulis_ext where dataset = 1;

drop table hermopoulis_standard1_200;
create table hermopoulis_standard1_200(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double, mobility string, place string, activity string)
    comment 'hermopoulis_standard with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets. sql: select concat_ws(-, userid, cast(dataset as string)), concat_ws(-, userid, cast(dataset as string)), unix_timestamp(sems1), latitude, longitude, sems2, sems3, sems4 from hermopoulis_ext where dataset = 1 and cast(userid as int) < 200'
    clustered by(userid) sorted by (`timestamp`) into 200 buckets
    stored as parquet;
insert into hermopoulis_standard1_200 select concat_ws('-', userid, cast(dataset as string)), concat_ws('-', userid, cast(dataset as string)), unix_timestamp(sems1), latitude, longitude, sems2, sems3, sems4 from hermopoulis_ext where dataset = 1 and cast(userid as int) < 200;

drop table hermopoulis_standard1_100;
create table hermopoulis_standard1_100(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double, mobility string, place string, activity string)
    comment 'hermopoulis_standard with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets. sql: select concat_ws(-, userid, cast(dataset as string)), concat_ws(-, userid, cast(dataset as string)), unix_timestamp(sems1), latitude, longitude, sems2, sems3, sems4 from hermopoulis_ext where dataset = 1 and cast(userid as int) < 100'
    clustered by(userid) sorted by (`timestamp`) into 200 buckets
    stored as parquet;
insert into hermopoulis_standard1_100 select concat_ws('-', userid, cast(dataset as string)), concat_ws('-', userid, cast(dataset as string)), unix_timestamp(sems1), latitude, longitude, sems2, sems3, sems4 from hermopoulis_ext where dataset = 1 and cast(userid as int) < 100;



drop table ctm.hermopoulis_standard;
drop table ctm.hermopoulis_ext;
drop table ctm.hermopoulis_standard1;
select * from trajectory.hermopoulis_standard limit 10;
select * from trajectory.hermopoulis_ext limit 10;
select * from trajectory.hermopoulis_standard1 limit 10;
use trajectory;
use ctm;

select * from hermopoulis_standard limit 10;

select tid, itemid, userid, latitude, longitude, time_bucket, semf from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_Infinity__size_800__sup_12__bins_19__ts_notime__bint_1;

use ctm;
select tid, latitude, longitude, time_bucket, semf, count(distinct itemid)
from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_600__sup_12__bins_19__ts_notime__bint_1
group by tid, latitude, longitude, time_bucket, semf
having count(distinct itemid) >= 1500
order by latitude, longitude, time_bucket, semf;

select * from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_600__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY limit 10;
describe ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_600__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY;
describe ctm.tmp_celltoid__tbl_hermopoulis_standard__lmt_1000000__size_600__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY;
describe ctm.tmp_neighborhood__tbl_hermopoulis_standard__lmt_1000000__size_600__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY;
describe ctm.CTM__tbl_hermopoulis_standard__lmt_1000000__size_600__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__summary;
describe ctm.CTM__tbl_hermopoulis_standard__lmt_1000000__size_600__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__itemset;
describe ctm.CTM__tbl_hermopoulis_standard__lmt_1000000__size_600__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__support;

select split(semf, '-')[0] from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_600__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY limit 10;
select * from ctm.CTM__tbl_hermopoulis_standard__lmt_1000000__size_600__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__summary order by support desc, size asc;

select userid, count(*) as c from trajectory.hermopoulis_standard group by userid order by c desc limit 1;
select dataset, count(*) as c from trajectory.hermopoulis_ext group by dataset order by c desc;


select min(time_bucket), max(time_bucket) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_absolute__bint_3600__semf_MOBILITYPLACEACTIVITY limit 10;
select t1, t2 from ctm.tmp_neighborhood__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_absolute__bint_3600__semf_MOBILITYPLACEACTIVITY limit 10;
describe ctm.tmp_neighborhood__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_absolute__bint_3600__semf_MOBILITYPLACEACTIVITY;

describe ctm.CTM__tbl_hermopoulis_standard__lmt_1000000__size_400__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__support;
describe ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_400__sup_12__bins_19__ts_notime__bint_1;
describe ctm.CTM__tbl_hermopoulis_standard__lmt_1000000__size_400__sup_12__bins_19__ts_notime__bint_1__semf_MOBILITYPLACEACTIVITY__epss_Infinity__epst_Infinity__freq_1__sthr_1000000__support;

select count(*) from trajectory.hermopoulis_standard;
select avg(c), stddev_pop(c) from (select userid, count(*) as c from trajectory.hermopoulis_standard group by userid) a;
select min(latitude), max(latitude), min(longitude), max(longitude) from trajectory.hermopoulis_standard; -- y = 27.84km x = 22.28km, area = 620km2

select count(distinct concat_ws('',latitude,longitude,time_bucket,semf) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_notime__bint_1__semf_mobilityplaceactivity;
select count(distinct concat_ws('',cast(latitude as string), cast(longitude as string))) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_absolute__bint_3600__semf_mobilityplaceactivity;
select count(distinct concat_ws('',cast(latitude as string), cast(longitude as string), cast(time_bucket as string),semf)) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_400__sup_12__bins_18__ts_absolute__bint_3600__semf_mobilityplaceactivity;
select count(distinct tid) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_400__sup_12__bins_17__ts_absolute__bint_3600__semf_mobilityplaceactivity;
select count(distinct tid) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_300__sup_12__bins_19__ts_absolute__bint_3600__semf_mobilityplaceactivity;

select cast(cast(latitude * 10000 / (11 * 19) as int) * (11 * 19) as int) / 10000 as latitude, cast(cast(longitude * 10000 / (13 * 19) as int) * (13 * 19) as int) / 10000  as longitude, count(distinct userid) as c
from trajectory.hermopoulis_standard
group by cast(cast(latitude * 10000 / (11 * 19) as int) * (11 * 19) as int) / 10000, cast(cast(longitude * 10000 / (13 * 19) as int) * (13 * 19) as int) / 10000
having c >= 500
order by 1, 2;

describe trajectory.hermopoulis_standard;

select count(*) from (
                         select round(round(latitude / (11 * 17), 4) * (11 * 17), 4) as latitude, round(round(longitude / (13 * 17), 4)  * (13 * 17), 4)  as longitude,
                                -- round(`timestamp` / 3600, 0) * 3600,
                                concat_ws('-', mobility, place, activity) as semf,
                                count(distinct userid) as c
                         from trajectory.hermopoulis_standard
                         group by round(round(latitude / (11 * 17), 4) * (11 * 17), 4), round(round(longitude / (13 * 17), 4)  * (13 * 17), 4)
                                -- , round(`timestamp` / 3600, 0) * 3600
                                , concat_ws('-', mobility, place, activity)
                         having c >= 100
                         order by 1, 2
                     ) a; -- 1476

select distinct latitude, longitude, time_bucket, semf from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_notime__bint_1 limit 10;
select distinct latitude, longitude, time_bucket, semf from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_absolute__bint_3600 limit 10;
select count(distinct tid) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_notime__bint_1;                                  --  40 (vs  47 vs  77)
select count(distinct tid) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_notime__bint_1__semf_mobilityplaceactivity;      -- 116 (vs 127 vs 306)
select count(distinct tid) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_absolute__bint_3600;                             -- 192 (vs 187 vs 4171)
select count(distinct tid) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_absolute__bint_3600__semf_mobilityplaceactivity; -- 150 (vs 196 vs 6907)

select count(distinct tid) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_19__ts_absolute__bint_3600__semf_mobilityplaceactivity; -- 150 (vs 6907)
select count(distinct tid) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_18__ts_absolute__bint_3600__semf_mobilityplaceactivity; -- 141 (vs 7605)
select count(distinct tid) from ctm.tmp_transactiontable__tbl_hermopoulis_standard__lmt_1000000__size_500__sup_12__bins_17__ts_absolute__bint_3600__semf_mobilityplaceactivity; -- 158 (vs 306)


select distinct time_bucket - 1 as time_bucket, tid from ctm.tmp_transactiontable__tbl_oldenburg_standard_2000_distinct__lmt_1000000__size_10__sup_35__bins_20__ts_absolute__bint_1 order by 1 asc, 2 asc;
select concat_ws(' ', collect_set(cast(tid as string))) from ctm.tmp_transactiontable__tbl_oldenburg_standard_2000_distinct__lmt_1000000__size_10__sup_35__bins_20__ts_absolute__bint_1 group by itemid;

select * from ctm__tbl_hermopoulis_standard_1000__lmt_1000000__size_14__sup_1__bins_19__ts_notime__bint_1__epss_infinity__epst_infinity__freq_1__sthr_1000000__summary;


select latitude, longitude, count(distinct itemsetid), 124 as total
from ctm__tbl_milan_standard__lmt_6000000__size_100__sup_12__bins_16__ts_daily__bint_4__epss_infinity__epst_infinity__freq_1__sthr_1000000__support a,
     tmp_transactiontable__tbl_milan_standard__lmt_6000000__size_100__sup_12__bins_16__ts_daily__bint_4 b
where a.tileid = b.tid
group by latitude, longitude;


select latitude, longitude, count(distinct itemsetid), 154 as total, count(distinct itemsetid) / 154 as avg
from ctm__tbl_milan_standard_1000__lmt_1000000__size_10__sup_1__bins_19__ts_notime__bint_1__epss_infinity__epst_infinity__freq_1__sthr_1000000__support a,
    tmp_transactiontable__tbl_milan_standard_1000__lmt_1000000__size_10__sup_1__bins_19__ts_notime__bint_1 b
where a.tileid = b.tid
group by latitude, longitude;

select count(distinct itemsetid) from ctm__tbl_milan_standard_1000__lmt_1000000__size_10__sup_1__bins_19__ts_notime__bint_1__epss_infinity__epst_infinity__freq_1__sthr_1000000__summary;