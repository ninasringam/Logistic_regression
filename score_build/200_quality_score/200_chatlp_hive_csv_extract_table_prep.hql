set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_dummy_tbls;

use ${QESDUMMYBLS};

create temporary macro nullif(s string, p string) if(s = p, null, s);

drop table if exists ${QESDUMMYBLS}.chatlp_events_temp_v1;
create table chatlp_events_temp_v1 stored as orc tblproperties("orc.compress" = "SNAPPY") as 
select a.*
, coalesce(b.churn_vol_m1_flag,0) as churn_vol_m1_flag
from chatlp_events_datamart as a
 left join ${QESDUMMYBLS}.customer_datamart as b
 on  a.mtn = b.mtn
 and a.month = b.month
where a.month between '202005' and '202009'
 and nullif(a.mtn,'') is not null
;

-- [Final Table] Aggregate all metrics individual and union into one big table for later csv extract
drop table if exists ${QESDUMMYBLS}.chatlp_aggregates_v1;
create table chatlp_aggregates_v1 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select cust_first_resptime_max as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'cust_first_resptime_max' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by cust_first_resptime_max

union all
select cust_first_resptime_avg as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'cust_first_resptime_avg' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by cust_first_resptime_avg

union all
select art_resptime_avg as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'art_resptime_avg' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by art_resptime_avg

union all
select art_resptime_sum as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'art_resptime_sum' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by art_resptime_sum

union all
select arr_q as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'arr_q' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by arr_q

union all
select arr_resptime_avg as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'arr_resptime_avg' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by arr_resptime_avg

union all
select arr_resptime_sum as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'arr_resptime_sum' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by arr_resptime_sum

union all
select duration as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'duration' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by duration

union all
select total_msg as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'total_msg' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by total_msg

union all
select total_rep as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'total_rep' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by total_rep

union all
select nbr_of_intents as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'nbr_of_intents' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by nbr_of_intents

union all
select csat_avg as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'csat_avg' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by csat_avg

union all
select agent_id_cnt as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'agent_id_cnt' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by agent_id_cnt

union all
select chat_connection_score as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, 'chat_connection_score' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1_test1
group by chat_connection_score
;


drop table if exists ${QESDUMMYBLS}.chatlp_aggregates_flag;
create table chatlp_aggregates_flag stored as orc tblproperties("orc.compress" = "SNAPPY") as
select agent_transfer_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'agent_transfer_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by agent_transfer_flag

union all
select return_to_queue_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'return_to_queue_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by return_to_queue_flag

union all
select closed_by_agent_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'closed_by_agent_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by closed_by_agent_flag

union all
select closed_by_customer_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'closed_by_customer_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by closed_by_customer_flag

union all
select closed_by_timeout_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'closed_by_timeout_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by closed_by_timeout_flag

union all
select sales_conversion_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'sales_conversion_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by sales_conversion_flag

union all
select bnc_3d_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'bnc_3d_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by bnc_3d_flag

union all
select retail_bnc_3d_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'retail_bnc_3d_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by retail_bnc_3d_flag

union all
select voice_bnc_3d_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'voice_bnc_3d_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by voice_bnc_3d_flag

union all
select ivr_bnc_3d_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'ivr_bnc_3d_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by ivr_bnc_3d_flag

union all
select web_bnc_3d_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'web_bnc_3d_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by  web_bnc_3d_flag

union all
select app_bnc_3d_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'app_bnc_3d_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by app_bnc_3d_flag

union all
select chatlp_bnc_3d_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'chatlp_bnc_3d_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by chatlp_bnc_3d_flag

union all
select chatbot_bnc_3d_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'chatbot_bnc_3d_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by chatbot_bnc_3d_flag

union all
select multiple_agent_flag as value
, 'all' as primary_high_level_reason
, count(*) as events
, sum(churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
, 'multiple_agent_flag' as metricname
from ${QESDUMMYBLS}.chatlp_events_temp_v1
group by  multiple_agent_flag
;

