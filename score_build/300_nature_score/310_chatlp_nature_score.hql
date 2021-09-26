set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_dummy_tbls;

use ${QESDUMMYBLS};
-- STEP 1: Use the existing universe of chat events to determine churn multipliers
-- 1.1 Establish the different churn rates
drop table if exists ${QESDUMMYBLS}.chatlp_nature_churn_lookup_v1;
create table chatlp_nature_churn_lookup_v1 stored as orc tblproperties('orc.compress' = 'SNAPPY') as 
select round(a.event_intensity_score,0) as intensity_score
, count(*) as events
, sum(b.churn_vol_m1_flag) as churn
, sum(b.churn_vol_m1_flag) / count(*) as churn_rate
from ${QESDUMMYBLS}.chatlp_events_intensity as a
left join ${QESDUMMYBLS}.customer_datamart as b
on a.mtn = b.mtn
and a.month = b.month
group by round(a.event_intensity_score,0)
;

-- Calculate the deltas to use
drop table if exists ${QESDUMMYBLS}.chatlp_nature_churn_lookup;
create table chatlp_nature_churn_lookup stored as orc tblproperties('orc.compress' = 'SNAPPY') as 
select
'Intensity score churn multipliers' as Note
, one.churn_rate / thr.churn_rate as delta_1
, two.churn_rate / thr.churn_rate as delta_2
, 1 as delta_3
, thr.churn_rate / fr.churn_rate as delta_4
, thr.churn_rate / fv.churn_rate as delta_5
from (select churn_rate from ${QESDUMMYBLS}.chatlp_nature_churn_lookup_v1 where intensity_score = 1) as one
cross join (select churn_rate from ${QESDUMMYBLS}.chatlp_nature_churn_lookup_v1 where intensity_score = 2) as two
cross join (select churn_rate from ${QESDUMMYBLS}.chatlp_nature_churn_lookup_v1 where intensity_score = 3) as thr
cross join (select churn_rate from ${QESDUMMYBLS}.chatlp_nature_churn_lookup_v1 where intensity_score = 4) as fr
cross join (select churn_rate from ${QESDUMMYBLS}.chatlp_nature_churn_lookup_v1 where intensity_score = 5) as fv
;

-- Apply churn delta multipliers to the actual events
drop table if exists ${QESDUMMYBLS}.chatlp_nature_scores_v1;
create table chatlp_nature_scores_v1 stored as orc tblproperties('orc.compress' = 'SNAPPY') as 
select  a.mtn
, a.event_intensity_score
, a.month
, case 
 when round(a.event_intensity_score,0) = 1 then (a.event_intensity_score - 3) * b.delta_1 
 when round(a.event_intensity_score,0) = 2 then (a.event_intensity_score - 3) * b.delta_2 
 when round(a.event_intensity_score,0) = 3 then (a.event_intensity_score - 3) * b.delta_3
 when round(a.event_intensity_score,0) = 4 then (a.event_intensity_score - 3) * b.delta_4
 when round(a.event_intensity_score,0) = 5 then (a.event_intensity_score - 3) * b.delta_5
 else null end
 as nature_score
 from ${QESDUMMYBLS}.chatlp_events_intensity as a
 cross join ${QESDUMMYBLS}.chatlp_nature_churn_lookup as b
;

-- Aggregate to customer monthly snapshot
drop table if exists ${QESDUMMYBLS}.chatlp_nature_scores;
create table chatlp_nature_scores stored as orc tblproperties('orc.compress' = 'SNAPPY') as 
select mtn
, month
, sum(nature_score) as nature_score
, count(*) as number_of_events
from ${QESDUMMYBLS}.chatlp_nature_scores_v1
group by 
mtn, month;
