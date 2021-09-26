set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_tst_tbls;

use ${QESDUMMYBLS};

create temporary macro nullif(s string, p string) if(s = p, null, s);

drop table if exists ${QESDUMMYBLS}.chatlp_agents_ids_v1;
create table chatlp_agents_ids_v1 stored as orc tblproperties('orc.compress' = 'SNAPPY') as
 select b.agent_id
 ,a.month
 ,a.mtn
 ,a.conversation_id
 ,a.chat_start_ts
 from qes_prdstg_tbls.chatlp_events_original a
 lateral view explode(list_of_transfer_agent) b as agent_id
;

drop table if exists ${QESDUMMYBLS}.chatlp_agents_scored_v1;
create table chatlp_agents_scored_v1 stored as orc tblproperties('orc.compress' = 'SNAPPY') as
select a.*
 ,b.primary_high_level_reason
 from chatlp_agents_ids_v1 as a
 left join chatlp_intents as b
 on cast(a.conversation_id as string) = cast(b.conversation_id as string)
 and a.mtn = b.mtn
;

drop table if exists ${QESDUMMYBLS}.chatlp_agents_scored_v2;
create table chatlp_agents_scored_v2 stored as orc tblproperties('orc.compress' = 'SNAPPY') as
select a.agent_id
 ,a.month
 ,a.mtn
 ,coalesce(nullif(a.primary_high_level_reason,''),'None') as primary_high_level_reason
 ,max(b.churn_vol_m1_flag) as churn_vol_m1_flag
 ,count(distinct conversation_id) as event_cnt
 from chatlp_agents_scored_v1 as a
 left join customer_datamart as b
 on a.mtn = b.mtn
 and a.month = b.month
 group by a.agent_id
 ,a.month
 ,a.mtn
 ,coalesce(nullif(a.primary_high_level_reason,''),'None')
;

drop table if exists ${QESDUMMYBLS}.chatlp_agents_scored_v3;
create table chatlp_agents_scored_v3 stored as orc tblproperties('orc.compress' = 'SNAPPY') as
select month
 ,agent_id
-- Collapsing typologies with small number of occurences
 ,case
 when primary_high_level_reason in ('Account Management'
 ,'Use'
 )
 then 'Account Management & Use'
 else primary_high_level_reason
 end as primary_high_level_reason
 ,sum(event_cnt) as event_cnt
 ,count(*) as dist_subs
 ,sum(churn_vol_m1_flag) as churn_m1
 ,sum(coalesce(churn_vol_m1_flag, 0)) / count(*) as real_churn_rate_m1
-- This adjusted column is needed for percentile calculation
 ,cast(
 (sum(coalesce(churn_vol_m1_flag,0)) / count(*)) * 10000
 as BIGINT) as churn_rate_m1
 from chatlp_agents_scored_v2
 group by month
 ,agent_id
 ,primary_high_level_reason
-- Filter out churn rates associated with insufficient event counts
 having event_cnt > 10
;

drop table if exists ${QESDUMMYBLS}.chatlp_agents_scored;
create table chatlp_agents_scored stored as orc tblproperties("orc.compress" = "SNAPPY") as
select a.month
 ,a.agent_id
 ,a.primary_high_level_reason
 ,a.dist_subs
 ,a.event_cnt
 ,a.churn_m1
 ,a.churn_rate_m1    / 10000 as churn_rate_m1
 ,b.min_churn_rate   / 10000 as min_churn_rate
 ,b.pct_1            / 10000 as p1_churn_rate
 ,b.pct_2            / 10000 as p2_churn_rate
 ,b.pct_5            / 10000 as p5_churn_rate
 ,b.pct_10           / 10000 as p10_churn_rate
 ,b.pct_30           / 10000 as p30_churn_rate
 ,b.pct_50           / 10000 as p50_churn_rate
 ,b.pct_70           / 10000 as p70_churn_rate
 ,b.pct_90           / 10000 as p90_churn_rate
 ,b.pct_95           / 10000 as p95_churn_rate
 ,b.pct_98           / 10000 as p98_churn_rate
 ,b.pct_99           / 10000 as p99_churn_rate
 ,b.max_churn_rate   / 10000 as max_churn_rate
 ,case
 when a.churn_rate_m1 > b.pct_95 then 1
 when a.churn_rate_m1 > b.pct_50
 and
 a.churn_rate_m1 <= b.pct_95 then 3 - ((a.churn_rate_m1 - b.pct_50) / (b.pct_95 - b.pct_50)) * 2
 when a.churn_rate_m1 > b.pct_5
 and
 a.churn_rate_m1 <= b.pct_50 then 5 - ((a.churn_rate_m1 - b.pct_5) / (b.pct_50 - b.pct_5)) * 2
 when a.churn_rate_m1 <= pct_5 then 5
 else null
 end as agent_score
-- This column will serve as a key in the join condition of the primary event datamart
 ,add_months(from_unixtime(unix_timestamp(month,'yyyyMM')), 1) as following_month
 from chatlp_agents_scored_v3 as a
 cross join (
 select primary_high_level_reason
 ,percentile(churn_rate_m1, 0.01)    as pct_1
 ,percentile(churn_rate_m1, 0.02)    as pct_2
 ,percentile(churn_rate_m1, 0.05)    as pct_5
 ,percentile(churn_rate_m1, 0.1)     as pct_10
 ,percentile(churn_rate_m1, 0.3)     as pct_30
 ,percentile(churn_rate_m1, 0.5)     as pct_50
 ,percentile(churn_rate_m1, 0.7)     as pct_70
 ,percentile(churn_rate_m1, 0.9)     as pct_90
 ,percentile(churn_rate_m1, 0.95)    as pct_95
 ,percentile(churn_rate_m1, 0.98)    as pct_98
 ,percentile(churn_rate_m1, 0.99)    as pct_99
 ,min(churn_rate_m1)                 as min_churn_rate
 ,max(churn_rate_m1)                 as max_churn_rate
 from chatlp_agents_scored_v3
 where month between 202005 and 202009
 group by primary_high_level_reason
 ) as b
 on a.primary_high_level_reason = b.primary_high_level_reason
;

drop table if exists ${QESDUMMYBLS}.chatlp_agent_quality_v1;
create table chatlp_agent_quality_v1 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select t1.conversation_id
 ,t1.mtn
 ,avg(t2.agent_score) as agent_score
 from (-- Need to re-extract agent_id for each event
 select a.*
 ,coalesce(nullif(b.primary_high_level_reason,''),'None') as primary_high_level_reason
 from chatlp_agents_ids_v1 as a
 left join chatlp_intents as b
 on  cast(a.conversation_id as string) = cast(b.conversation_id as string)
 and a.mtn = b.mtn
 ) as t1
-- Assign agent scores to the subsequent month's interactions
 left join chatlp_agents_scored  as t2
 on t1.agent_id = t2.agent_id
-- Get previous month's agent score to map it to agent at the current month's interactions
 and t1.month = CONCAT (substr(t2.following_month, 0, 4),substr(t2.following_month, 6, 2))  
 and case
 when t1.primary_high_level_reason in ('Account Management'
 ,'Use')
 then 'Account Management & Use'
 else t1.primary_high_level_reason
 end = t2.primary_high_level_reason
 group by t1.conversation_id
 ,t1.mtn
;
