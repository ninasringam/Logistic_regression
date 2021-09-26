set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_dummy_tbls;

use ${QESDUMMYBLS};

-- Depdendencies: 140_chatlp_features_intent.hql [chatlp_intents]

-- Re-duplicate events where there are multiple intent labels for a session
drop table if exists ${QESDUMMYBLS}.chatlp_churn_v1;
create table chatlp_churn_v1 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select b.low_level_intent
, a.*
 from chatlp_intents a
 lateral view explode(list_of_ctgry_driver) b as low_level_intent
;

-- Churn rate by intent
drop table if exists ${QESDUMMYBLS}.chatlp_churn_v2;
create table chatlp_churn_v2 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select a.low_level_intent as low_level_intent
, count(*) as number_of_instances
, (sum(c.churn_vol_m1_flag)) as churn_m1
 from ${QESDUMMYBLS}.chatlp_churn_v1 as a
 left join ${QESDUMMYBLS}.customer_datamart as c
 on  a.mtn = c.mtn 
 and a.month = c.month
 where c.mtn is not null
 group by a.low_level_intent    
;

-- Average overall churn rate
drop table if exists ${QESDUMMYBLS}.chatlp_average_churn_v1;
create table chatlp_average_churn_v1 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select (sum(churn_m1))/sum(number_of_instances) as avg_churn_m1 
from ${QESDUMMYBLS}.chatlp_churn_v2
;

----------------------------------------------------------------------
-- ... since median function requires an integer, we have to convert churn rate by multiplying it.
-- In order to keep two decimal point percentages, we will multiply by 10,000
-- if less than 10000 instances then replace churn rate from the typology average churn rate
drop table if exists ${QESDUMMYBLS}.chatlp_churn_v3;
create table chatlp_churn_v3 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select  i.low_level_intent as low_level_intent
, i.number_of_instances as number_of_instances
, i.churn_m1 as churn_m1
, (case when i.number_of_instances >= 10000 
then cast(((i.churn_m1 / i.number_of_instances) * 10000) as BIGINT)
 else cast((t.avg_churn_m1 * 10000) as BIGINT) 
 end) as churn_rate_m1
 from ${QESDUMMYBLS}.chatlp_churn_v2 i
 left join ${QESDUMMYBLS}.chatlp_average_churn_v1 t
;

-- group all below 10000 into a single category *** change 10000 to 10 ***
drop table if exists ${QESDUMMYBLS}.chatlp_churn_v4;
create table chatlp_churn_v4 stored as orc tblproperties("orc.compress" = "SNAPPY") as
-- select (case when number_of_instances < 10000 
select (case when number_of_instances < 10 
 then 'Other flows below 10' 
 else low_level_intent 
 end) as low_level_intent
 , sum(number_of_instances) as number_of_instances
 , sum(churn_m1) as churn_m1
 , max(churn_rate_m1) as churn_rate_m1 -- just the ones with less than 10000 get their churn replaced with the average (they already all have the average)
 from ${QESDUMMYBLS}.chatlp_churn_v3
-- group by (case when number_of_instances < 10000
 group by (case when number_of_instances < 10
-- then 'Other flows below 10000' 
 then 'Other flows below 10' 
 else low_level_intent end);

drop table if exists ${QESDUMMYBLS}.chatlp_churn_v5;
create table chatlp_churn_v5 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select a.low_level_intent
, a.number_of_instances
, a.churn_m1
, a.churn_rate_m1 / 10000 as churn_rate_m1
, b.pct_1 / 10000   as p1_churn_rate
, b.pct_2 / 10000   as p2_churn_rate
, b.pct_5 / 10000   as p5_churn_rate
, b.pct_10 / 10000  as p10_churn_rate
, b.pct_30 / 10000  as p30_churn_rate
, b.pct_50 / 10000  as p50_churn_rate
, b.pct_70 / 10000  as p70_churn_rate
, b.pct_90 / 10000  as p90_churn_rate
, b.pct_95 / 10000  as p95_churn_rate
, b.pct_98 / 10000  as p98_churn_rate
, b.pct_99 / 10000  as p99_churn_rate
, b.max_churn_rate / 10000 as max_churn_rate
, case 
 when a.churn_rate_m1 > b.pct_50 then  case when (3 - ( (a.churn_rate_m1 - b.pct_50) / (b.pct_90 - b.pct_50)) * 2) >= 1 
 then 3 - ( (a.churn_rate_m1 - b.pct_50) / (b.pct_90 - b.pct_50)) * 2 else 1 end
 when a.churn_rate_m1 = b.pct_50 then 3
 when a.churn_rate_m1 < b.pct_50 then case when (3 + ( ( b.pct_50 - a.churn_rate_m1) / ( b.pct_50 - b.pct_10)) * 2) <= 5 then
 3 + ( (b.pct_50 - a.churn_rate_m1) / ( b.pct_50 - b.pct_10)) * 2 else 5 end
 else null end as intensity_score
 from ${QESDUMMYBLS}.chatlp_churn_v4 as a
 cross join 
 (select
 percentile(churn_rate_m1, 0.01) as pct_1
 , percentile(churn_rate_m1, 0.02) as pct_2
 , percentile(churn_rate_m1, 0.05) as pct_5
 , percentile(churn_rate_m1, 0.1) as pct_10 
 , percentile(churn_rate_m1, 0.3) as pct_30
 , percentile(churn_rate_m1, 0.5) as pct_50
 , percentile(churn_rate_m1, 0.7) as pct_70
 , percentile(churn_rate_m1, 0.9) as pct_90
 , percentile(churn_rate_m1, 0.95) as pct_95
 , percentile(churn_rate_m1, 0.98) as pct_98
 , percentile(churn_rate_m1, 0.99) as pct_99
 , min(churn_rate_m1) as min_churn_rate
 , max(churn_rate_m1) as max_churn_rate
 from ${QESDUMMYBLS}.chatlp_churn_v4
 ) as b
;

-- flow intensity lookup
drop table if exists ${QESDUMMYBLS}.chatlp_intensity_lookup;
create table chatlp_intensity_lookup stored as orc tblproperties("orc.compress" = "SNAPPY") as
 select a.*
, abs(3.0 - intensity_score) as extremeness_weight
, (abs(3.0 - intensity_score) * intensity_score) as extremeness_score
 from ${QESDUMMYBLS}.chatlp_churn_v5 as a;
 
drop table if exists ${QESDUMMYBLS}.chatlp_events_intensity_v1;
create table chatlp_events_intensity_v1 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select b.low_level_intent
, a.*
 from ${QESDUMMYBLS}.chatlp_intents a
 lateral view explode(list_of_ctgry_driver) b as low_level_intent;
 
-- Aggregate the average intensity score using weights defined by their distance to 3 (calculated in the table above)
drop table if exists ${QESDUMMYBLS}.chatlp_events_intensity_v2;
create table chatlp_events_intensity_v2 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select e.mtn
, e.conversation_id
, e.month
-- , e.chat_start_dt 
-- when ii.low_level_intent = 'Other flows below 10000' then 1 else 0 end) as flow_below_10000
, cast(cast(upper(i.extremeness_score) as string) as double) / cast(cast(upper(i.extremeness_weight) as string) as double) as chat_intensity_score 
from ${QESDUMMYBLS}.chatlp_events_intensity_v1 as e
 left join ${QESDUMMYBLS}.chatlp_intensity_lookup as i
 on trim(lower(e.low_level_intent)) = trim(lower(i.low_level_intent))
-- left join ${QESDUMMYBLS}.chatlp_intensity_lookup as ii
-- on  e.low_level_intent is not null 
-- and i.low_level_intent is null 
-- and ii.low_level_intent = 'Other flows below 10000'
-- and ii.low_level_intent = 'Other flows below 10'
where e.mtn is not null;

-- group by msisdn
drop table if exists ${QESDUMMYBLS}.chatlp_events_intensity;
create table chatlp_events_intensity stored as orc tblproperties("orc.compress" = "SNAPPY") as
select mtn
, month
, conversation_id
-- , (case when sum(flow_below_10000) > 0 
-- then 1 else 0 end) as flow_below_10000
, avg(chat_intensity_score) as event_intensity_score
from ${QESDUMMYBLS}.chatlp_events_intensity_v2
group by mtn, month, conversation_id;

