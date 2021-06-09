-- Databricks notebook source
drop table if exists dc_data_raw;
create table dc_data_raw
using parquet
options(
  PATH '/FileStore/tables/data_centers_q2_q3_snappy.parquet'
);

-- COMMAND ----------

SELECT * FROM dc_data_raw limit 1 ;

-- COMMAND ----------

describe detail dc_data_raw;

-- COMMAND ----------

select explode (source)
from dc_data_raw;

-- COMMAND ----------

with explode_source
as
  (
  select
  dc_id,
  to_date(date) as date,
  explode (source)
  from dc_data_raw
  )
select key,
  dc_id,
  date,
  value.description,
  value.ip,
  value.temps,
  value.co2_level
from explode_source;

-- COMMAND ----------

drop table if exists device_data;

create table device_data
using delta
partitioned by (device_type)
with explode_source
as
  (
  select 
  dc_id,
  to_date(date) as date,
  explode (source)
  from dc_data_raw 
  )
select 
  dc_id,
  key `device_type`,
  date,
  value.description,
  value.ip,
  value.temps,
  value.co2_level
from explode_source;

select * from device_data

-- COMMAND ----------

describe extended device_data

-- COMMAND ----------

cache table device_data

-- COMMAND ----------

SELECT 
  dc_id,
  device_type, 
  temps,
  TRANSFORM (temps, t -> ((t * 9) div 5) + 32 ) AS `temps_F`
FROM device_data;

-- COMMAND ----------

create or replace temporary view co2_level_temporary
as
  select 
    dc_id,
    device_type,
    co2_level,
    reduce (co2_level, 0, (c,acc) -> c + acc,acc ->(acc div size(co2_level))) as average_co2_level
  from device_data
  sort by average_co2_level desc;
select * from co2_level_temporary

-- COMMAND ----------

select * from(
  select device_type, average_co2_level
  from co2_level_temporary
) 

pivot(
  round (AVG(average_co2_level), 2) as avg_co2
  for device_type in ('sensor-ipad', 'sensor-inest',
  'sensor-istick','sensor-igauge')
  );

-- COMMAND ----------

select 
  dc_id,
  device_type,
  round (avg(average_co2_level)) as avg_co2_level
from co2_level_temporary
group by rollup (dc_id,device_type)
order by dc_id, device_type

-- COMMAND ----------

select 
 coalesce (dc_id,'all data centers') as dc_id,
 coalesce (device_type, 'all devices') as device_type,
 ROUND (AVG(average_co2_level)) as avg_co2_level
from co2_level_temporary
group by rollup (dc_id,device_type)
order by dc_id, device_type

-- COMMAND ----------

select 
 coalesce (dc_id,'all data centers') as dc_id,
 coalesce (device_type, 'all devices') as device_type,
 ROUND (AVG(average_co2_level)) as avg_co2_level
from co2_level_temporary
group by CUBE (dc_id,device_type)
order by dc_id, device_type;

-- COMMAND ----------

create table if not exists avg_temps
using delta
partitioned by (device_type)
as 
  select 
    dc_id,
    date,
    temps,
    reduce (temps,0,(t, acc) -> t + acc, acc -> (acc div size (temps))) as avg_daily_temp_C,
    device_type
  from device_data;
select * from avg_temps;

-- COMMAND ----------

show partitions  avg_temps

-- COMMAND ----------

select 
  dc_id,
  date,
  avg_daily_temp_C,
  AVG(avg_daily_temp_C)
  over (partition by month(date),dc_id) as average_monthly_temp
from avg_temps
where month(date) = '8' and dc_id = 'dc-102';

-- COMMAND ----------

with diff_chart as
(
select 
  dc_id,
  date,
  avg_daily_temp_C,
  AVG(avg_daily_temp_C)
  over ( partition by month(date),dc_id)  as average_monthly_temp_C
from avg_temps
)


select 
  dc_id,
  date,
  avg_daily_temp_C
  average_monthly_temp_C,
  avg_daily_temp_C - round (average_monthly_temp_C) as degree_diff
from diff_chart

-- COMMAND ----------


