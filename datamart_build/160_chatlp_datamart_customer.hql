set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_tst_tbls;

use ${QESDUMMYBLS};

drop table if exists ${QESDUMMYBLS}.chatlp_customer_datamart;
create table chatlp_customer_datamart stored as orc tblproperties("orc.compress" = "SNAPPY") as
select mtn 
, month
-- Interaction count
,count(*) as event_cnt
-- Interaction time
,avg(cust_first_resptime_max) as avg_cust_first_resptime_max
,avg(cust_first_resptime_avg) as avg_cust_first_resptime_avg
,avg(art_resptime_avg) as avg_art_resptime_avg
,avg(art_resptime_sum) as avg_art_resptime_sum
,avg(arr_q) as avg_arr_q
,avg(arr_resptime_avg) as avg_arr_resptime_avg
,avg(arr_resptime_sum) as avg_arr_resptime_sum
,avg(duration) as avg_duration
,avg(total_msg) as avg_total_msg
,avg(total_rep) as avg_total_rep
,avg(chat_connection_score) as avg_chat_connection_score
-- Successful interactions
,sum(agent_transfer_flag) as agent_transfer_cnt
,sum(return_to_queue_flag) as return_to_queue_cnt
,sum(closed_by_agent_flag) as closed_by_agent_cnt
,sum(closed_by_customer_flag) as closed_by_customer_cnt
,sum(closed_by_timeout_flag) as closed_by_timeout_cnt
,sum(sales_conversion_flag) as sales_conversion_cnt
,sum(bnc_3d_flag) as bnc_3d_cnt
,sum(retail_bnc_3d_flag) as retail_bnc_3d_cnt
,sum(voice_bnc_3d_flag) as voice_bnc_3d_cnt
,sum(ivr_bnc_3d_flag) as ivr_bnc_3d_cnt
,sum(web_bnc_3d_flag) as web_bnc_3d_cnt
,sum(app_bnc_3d_flag) as app_bnc_3d_cnt
,sum(chatlp_bnc_3d_flag) as chatlp_bnc_3d_cnt
,sum(chatbot_bnc_3d_flag) as chatbot_bnc_3d_cnt
-- typology
,sum(nbr_of_intents) as nbr_of_intents
,concat_ws('\;',collect_set(primary_high_level_reason)) as primary_high_level_reason_set
-- Feedback variables
,avg(nps_avg) as avg_nps_avg
,avg(csat_avg) as avg_csat_avg
,avg(custom_avg) as avg_custom_avg
,sum(promoter_flag) as promoter_cnt
,sum(detractor_flag) as detractor_cnt
-- Agent information
,sum(agent_id_cnt) as total_agent_id_cnt
,sum(multiple_agent_flag) as events_with_multiple_agents_cnt 
from chatlp_events_datamart
group by mtn, 
month
;
