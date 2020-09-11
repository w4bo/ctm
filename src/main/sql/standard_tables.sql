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
select count(*) from geolife_standard;

-- CREATE TABLE cariploenr_standard
create table cariploenr_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double, accuracy int)
comment 'Cariploenr6 with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets. sql: select customid as userid, timest as timestamp, latitude, longitude, accuracy, trajid as trajectoryid from cariploenr6'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
-- partitioned by (userid string)
insert into cariploenr_standard select customid as userid, trajid as trajectoryid, timest as `timestamp`, latitude, longitude, accuracy from cariploenr6;

-- CREATE TABLE milano_standard
drop table milan_standard;
create table milan_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double)
comment 'milan_standard with standard schema clustered by(userid) sorted by (`timestamp`) into 200 buckets. sql: select userid, trajectoryid, `timestamp`, latitude, longitude from cariploenr_standard where latitude >= 45.4 and latitude <= 45.5 and longitude >= 9.04 and longitude <= 9.275'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
insert into milan_standard select userid, trajectoryid, `timestamp`, latitude, longitude from cariploenr_standard where latitude >= 45.4 and latitude <= 45.5 and longitude >= 9.04 and longitude <= 9.275;

-- CREATE TABLE oldenburg_standard
create table oldenburg_standard(userid string, trajectoryid string, `timestamp` bigint, latitude double, longitude double, accuracy int)
comment 'Oldenburg with 1M trajectories.'
clustered by(userid) sorted by (`timestamp`) into 200 buckets
stored as parquet;
insert into oldenburg_standard select customid as userid, trajid as trajectoryid, timest as `timestamp`, latitude, longitude, accuracy from cariploenr6;


hive
use trajectory;

-- filter valid trajectories
drop table tmp_trajFiltered;
create table tmp_trajFiltered as
    select *
    from(
        select *, max(pointOrder) over(partition by customid, trajid) trajPointCount
        from (
            select *,
                avg(spacediff) over(partition by customid, trajid) spaceMedia,
                avg(timediff) over(partition by customid, trajid) timeMedia,
                avg(speed) over(partition by customid, trajid) speedMedia,
                ROW_NUMBER() over(partition by customid, trajid ORDER BY timest) AS pointOrder
            from cariploenewfiltermilanonly
            where speed > 0
        ) a
        where spaceMedia > 0.01 and speedMedia > 25 -- 15
    ) b
    where b.trajPointCount > 25;

-- get N trajectories with enough points inside Milano
drop table tmp_top50trajPointInsideMilanCenter;
create table tmp_top50trajPointInsideMilanCenter as
    select customid, trajid, count(timest) as numPointOfTrajInside
    from cariploenewfiltermilanonly t
    where speed > 0 and latitude between 45.4370 and 45.5020 and longitude between 9.1344 and 9.2344
    group by customid, trajid
    order by numPointOfTrajInside DESC;
    -- limit 1000;

-- merge the two tables
drop table trajectoryflow_besttrj;
create table trajectoryflow_besttrj as
    select t.*
    from tmp_trajFiltered t JOIN tmp_top50trajPointInsideMilanCenter t2 ON (t.customid = t2.customid AND t.trajid = t2.trajid);

create table geolife_bejin as select * from geolifeext where (latitude between 39.69 and 40.2) and (longitude between 116.1 and 116.7);

drop table tmp_trajflow_debug;
create table tmp_trajflow_debug as
    select cell, count(distinct trajid) as cdistinct, count(trajid) as c
    from trajectoryflow_besttrj_mapped
    where trajid
    group by cell
    order by c desc;

drop table trajectoryflow_besttrj_stats;
create table trajectoryflow_besttrj_stats as
    select trajid, count(distinct cell) as trajpointcount
    from trajectoryflow_besttrj_mapped
    group by trajid;

-- convert to string
drop table trajectoryflow_besttrjtoprint;
create table trajectoryflow_besttrjtoprint as
    select customid, trajid, concat(customid, "\;", trajid, "\;", concat_ws(",", collect_set(concat(latitude, "_", longitude)))) as tostring
    from trajectoryflow_besttrj
    group by customid, trajid;

exit;

hive -e 'use trajectory; select tostring from trajectoryflow_besttrjtoprint' | sed 's/[\t]/,/g'  > milanogpstraces.csv
hive -e "use trajectory; select concat(customid, '_', trajid), latitude, longitude, timest from trajectoryflow_besttrj" | sed 's/[\t]/,/g'  > milanogpstraces2.csv

hive
use trajectory;
drop table tmp_trajFiltered;
drop table tmp_top50trajPointInsideMilanCenter;
-- drop table trajectoryflow_besttrj; mi servono anche per la parte di clustering su spark, non solo per essere stampate per k-medoids
drop table trajectoryflow_besttrjtoprint;
exit;

-- extract itemsets
hive -e 'set hive.cli.print.header=true; use trajectory; select size * support, size, itemset from trajectoryflow_fis_1200_1000 order by size desc' | sed 's/[\t]/,/g'  > trajflowfis.csv
-- extracts all trajectories
hive -e 'set hive.cli.print.header=true; use trajectory; select trajid, cell, latitude, longitude, timest  from trajectoryflow_besttrj_mapped order by trajid asc, timest asc' | sed 's/[\t]/,/g'  > traj_all.csv
-- extracts trajectory distribution over cells
hive -e 'set hive.cli.print.header=true; use trajectory; select distinct 1 as cellround, t.cell, t.cid, t.latitude, t.longitude, t2.trajid from trajectoryflow_besttrj_celltoid1 t, trajectoryflow_besttrj_mapped1 t2 where t.cell = t2.cell' | sed 's/[\t]/,/g'  > traj_incell.csv
hive -e                                 'use trajectory; select distinct 2 as cellround, t.cell, t.cid, t.latitude, t.longitude, t2.trajid from trajectoryflow_besttrj_celltoid2 t, trajectoryflow_besttrj_mapped2 t2 where t.cell = t2.cell' | sed 's/[\t]/,/g'  >> traj_incell.csv
hive -e                                 'use trajectory; select distinct 3 as cellround, t.cell, t.cid, t.latitude, t.longitude, t2.trajid from trajectoryflow_besttrj_celltoid3 t, trajectoryflow_besttrj_mapped3 t2 where t.cell = t2.cell' | sed 's/[\t]/,/g'  >> traj_incell.csv
hive -e 'set hive.cli.print.header=true; use trajectory; select distinct 2 as cellround, t.cell, t.cid, t.latitude, t.longitude, t2.trajid from trajectoryflow_geolife_celltoid2 t, trajectoryflow_geolife_mapped2 t2 where t.cell = t2.cell' | sed 's/[\t]/,/g'  > geolife_incell.csv

-- extracts cluster with their respective trajectories
hive -e 'set hive.cli.print.header=true; use trajectory; select t1.trajid, t1.cell, t1.latitude, t1.longitude, t1.timest, t2.clusterid, t2.size, t2.cohesion, t2.cohesion2, t2.support from tmp_tf_mapped_besttrj_10_100 t1, (select * from trajectoryflow_cluster_besttrj_100_1_10_Infinity_1 where cohesion > 0.35) t2 where t1.trajid = t2.trajid' | sed 's/[\t]/,/g'  > traj_incluster3.csv
