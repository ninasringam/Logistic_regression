set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_tst_tbls;

use ${QESDUMMYBLS};

-- Rescaling event metrics
drop table if exists ${QESDUMMYBLS}.chatlp_events_scored_v1;
create table chatlp_events_scored_v1 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select a.*
------------------------- TIME-BASED FEATURES -------------------------
-- , case 
--  when cust_first_resptime_max > 717.00 then 5
--  when cust_first_resptime_max > 716.05 and cust_first_resptime_max <= 717.00 then 4 + (717.00 - cust_first_resptime_max) / (717.00 - 716.05)
--  when cust_first_resptime_max > 20.00 and cust_first_resptime_max <= 716.05 then 3 + (716.05 - cust_first_resptime_max) / (716.05 - 20.00)
--  when cust_first_resptime_max > 3.19 and cust_first_resptime_max <= 20.00 then 2 + (20.00 - cust_first_resptime_max) / (20.00 - 3.19)
--  when cust_first_resptime_max > 3.00 and cust_first_resptime_max <= 3.19 then 1 + (3.19 - cust_first_resptime_max) / (3.19 - 3.00)
--  when cust_first_resptime_max <= 3.00 then 1 
--  end as q_cust_first_resptime_max
-- , case 
--  when cust_first_resptime_avg > 715.00 then 5
--  when cust_first_resptime_avg > 20.63 and cust_first_resptime_avg <= 715.00 then 4 + (715.00 - cust_first_resptime_avg) / (715.00 - 20.63)
--  when cust_first_resptime_avg > 20.00 and cust_first_resptime_avg <= 20.63 then 3 + (20.63 - cust_first_resptime_avg) / (20.63 - 20.00)
--  when cust_first_resptime_avg > 3.19 and cust_first_resptime_avg <= 5.00 then 2 + (5.00 - cust_first_resptime_avg) / (5.00 - 3.19)
--  when cust_first_resptime_avg > 3.00 and cust_first_resptime_avg <= 3.19 then 1 + (3.19 - cust_first_resptime_avg) / (3.19 - 3.00)
--  when cust_first_resptime_avg <= 3.00 then 1 
--  end as q_cust_first_resptime_avg 
, case 
 when arr_resptime_sum > 1254.00 then 1
 when arr_resptime_sum > 77.58 and arr_resptime_sum <= 1254.00 then 1 + (1254.00 - arr_resptime_sum) / (1254.00 - 77.58)
 when arr_resptime_sum > 61.00 and arr_resptime_sum <= 77.58 then 2 + (77.58 - arr_resptime_sum) / (77.58 - 61.00)
 when arr_resptime_sum > 5.79 and arr_resptime_sum <= 61.00 then 3 + (61.00 - arr_resptime_sum) / (61.00 - 5.79)
 when arr_resptime_sum > 0 and arr_resptime_sum <= 5.79 then 4 + (5.79 - arr_resptime_sum) / (5.79 - 0)
 when arr_resptime_sum = 0 then 5 
 end as q_arr_resptime_sum
 , case 
 when arr_resptime_avg > 330.00 then 5
 when arr_resptime_avg > 40.52 and arr_resptime_avg <= 330.00 then 4 + (330.00 - arr_resptime_avg) / (330.00 - 40.52)
 when arr_resptime_avg > 37.00 and arr_resptime_avg <= 40.52 then 3 + (40.52 - arr_resptime_avg) / (40.52 - 37.00)
 when arr_resptime_avg > 3.83 and arr_resptime_avg <= 37.00 then 2 + (37.00 - arr_resptime_avg) / (37.00 - 3.83)
 when arr_resptime_avg > 0 and arr_resptime_avg <= 3.83 then 1 + (3.83 - arr_resptime_avg) / (3.83 - 0)
 when arr_resptime_avg = 0 then 1 
 end as q_arr_resptime_avg
-- , case 
-- when art_resptime_avg > 414.00 then 5
-- when art_resptime_avg > 413.00 and art_resptime_avg <= 414.00 then 4 + (414.00 - art_resptime_avg) / (414.00 - 413.00)
-- when art_resptime_avg > 20.00 and art_resptime_avg <= 413.00 then 3 + (413.00 - art_resptime_avg) / (413.00 - 20.00)
-- when art_resptime_avg > 3.72 and art_resptime_avg <= 20.00 then 2 + (20.00 - art_resptime_avg) / (20.00 - 3.72)
-- when art_resptime_avg > 3.00 and art_resptime_avg <= 3.72 then 1 + (3.72 - art_resptime_avg) / (3.72 - 3.00)
-- when art_resptime_avg <= 3.00 then 1 
-- end as q_art_resptime_avg 
, case 
 when total_msg > 54.0 then 1
 when total_msg > 12.0 and total_msg <= 54.0 then 1 + (54.0 - total_msg) / (54.0 - 12.0)
 when total_msg > 11.0 and total_msg <= 12.0 then 2 + (12.0 - total_msg) / (12.0 - 11.0)
 when total_msg > 3.0 and total_msg <= 11.0 then 3 + (11.0 - total_msg) / (11.0 - 3.0)
 when total_msg > 2.0 and total_msg <= 3.0 then 4 + (3.0 - total_msg) / (3.0 - 2.0)
 when total_msg <= 2.0 then 5 
 end as q_total_msg
, case 
 when arr_q > 472.0 then 1
 when arr_q > 2.47 and arr_q <= 472.0 then 1 + (472.0 - arr_q) / (472.0 - 2.47)
 when arr_q > 2.0 and arr_q <= 2.47 then 2 + (2.47 - arr_q) / (2.47 - 2.0)
 when arr_q > 1.41 and arr_q <= 2.0 then 3 + (2.0 - arr_q) / (2.0 - 1.41)
 when arr_q > 0.0 and arr_q <= 1.41 then 4 + (1.41 - arr_q) / (1.41 - 0.0)
 when arr_q <= 0.0 then 5 
 end as q_arr_q
, case 
 when chat_connection_score > 39.0 then 5
 when chat_connection_score > 0.0 and chat_connection_score <= 39.0 then 4 + (39.0 - chat_connection_score) / (39.0 - 0.0)
 when chat_connection_score > -3.0 and chat_connection_score <= 0.0 then 3 + (0.0 - chat_connection_score) / (0.0 + 3.0)
 when chat_connection_score > -6.0 and chat_connection_score <= -3.0 then 2 + (-3.0 - chat_connection_score) / (-3.0 + 6.0)
 when chat_connection_score > -33.0 and chat_connection_score <= -6.0 then 1 + (-6.0 - chat_connection_score) / (-6.0 + 33.0)
 when chat_connection_score <= -33.0 then 1  
 end as q_chat_connection_score
------------------------ EFFORT-BASED FEATURES ------------------------
-- Multi-agent flag: Whether more than one agent was involved in the chat
, case
 when ivr_bnc_3d_flag = 1 then 0
 when ivr_bnc_3d_flag = 0 then 1
 end as q_ivr_bnc_3d_flipped_flag
-- closed_by_customer_flag
, case
 when closed_by_customer_flag  = 1 then 0
 when closed_by_customer_flag  = 0 then 1
 end as q_closed_by_customer_flipped_flag
-- return_to_queue_flag 
, case
 when return_to_queue_flag = 1 then 0
 when return_to_queue_flag = 0 then 1
 end as q_return_to_queue_flipped_flag
-- web_bnc_3d_flag
, case
 when web_bnc_3d_flag = 1 then 0
 when web_bnc_3d_flag = 0 then 1
 end as q_web_bnc_3d_flipped_flag
-- voice_bnc_3d_flag
, case
 when voice_bnc_3d_flag = 1 then 0
 when voice_bnc_3d_flag = 0 then 1
 end as q_voice_bnc_3d_flipped_flag
-- bnc_3d_flag 
, case
 when bnc_3d_flag = 1 then 0
 when bnc_3d_flag = 0 then 1
 end as q_bnc_3d_flipped_flag
-- closed_by_timeout_flag
, case
 when closed_by_timeout_flag = 1 then 0
 when closed_by_timeout_flag = 0 then 1
 end as q_closed_by_timeout_flipped_flag 
-- agent_transfer_flag 
, case
 when agent_transfer_flag = 1 then 1
 when agent_transfer_flag = 0 then 0
 end as q_agent_transfer_flag
-- multiple_agent_flag
, case
 when multiple_agent_flag = 1 then 1
 when multiple_agent_flag = 0 then 0
 end as q_multiple_agent_flag
-- app_bnc_3d_flag 
, case
 when app_bnc_3d_flag = 1 then 1
 when app_bnc_3d_flag = 0 then 0
 end as q_app_bnc_3d_flag
-- retail_bnc_3d_flag
, case
 when retail_bnc_3d_flag = 1 then 1
 when retail_bnc_3d_flag = 0 then 0
 end as q_retail_bnc_3d_flag 
-- sales_conversion_flag
, case
 when sales_conversion_flag = 1 then 1
 when sales_conversion_flag = 0 then 0
 end as q_sales_conversion_flag 
-- closed_by_agent_flag
, case
 when closed_by_agent_flag = 1 then 1
 when closed_by_agent_flag = 0 then 0
 end as q_closed_by_agent_flag 
--  chatbot_bnc_3d_flag
, case
 when chatbot_bnc_3d_flag = 1 then 1
 when chatbot_bnc_3d_flag = 0 then 0
 end as q_chatbot_bnc_3d_flag 
--  chatlp_bnc_3d_flag
, case
 when chatlp_bnc_3d_flag = 1 then 0
 when chatlp_bnc_3d_flag = 0 then 1
 end as q_chatlp_bnc_3d_flipped_flag
------------------------ FEEDBACK-BASED FEATURES ------------------------
, case 
 when promoter_flag = 'N' then 0
 when promoter_flag = 'Y' then 1
 end as q_promoter_flag
, case 
 when detractor_flag = 'Y' then 0
 when detractor_flag = 'N' then 1
 end as q_detractor_flipped_flag
------------------------ AGENT SCORES ------------------------
, agent_score as q_agent_score
 from chatlp_events_datamart as a;
 
-- Aggregating to monthly customer snapshot
drop table if exists ${QESDUMMYBLS}.chatlp_customer_scored_v1;
create table chatlp_customer_scored_v1 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select a.*
, coalesce(b.churn_vol_m1_flag,0) as churn_vol_m1_flag
 from (select month
 , mtn
-- Continuous Scores
-- , avg(q_cust_first_resptime_max) as q_cust_first_resptime_max
-- , avg(q_cust_first_resptime_avg) as q_cust_first_resptime_avg 
 , avg(q_arr_resptime_sum) as q_arr_resptime_sum
 , avg(q_arr_resptime_avg) as q_arr_resptime_avg
-- , avg(q_art_resptime_avg) as q_art_resptime_avg
 , avg(q_total_msg) as q_total_msg
 , avg(q_arr_q) as q_arr_q
 , avg(q_chat_connection_score) as q_chat_connection_score
 , avg(q_agent_score) as q_agent_score
-- Regular flag
 , max(q_agent_transfer_flag) as q_agent_transfer_flag
 , max(q_multiple_agent_flag) as q_multiple_agent_flag 
 , max(q_app_bnc_3d_flag) as q_app_bnc_3d_flag
 , max(q_retail_bnc_3d_flag) as q_retail_bnc_3d_flag 
 , max(q_chatbot_bnc_3d_flag) as q_chatbot_bnc_3d_flag 
 , max(q_sales_conversion_flag ) as q_sales_conversion_flag  
 , max(q_closed_by_agent_flag) as q_closed_by_agent_flag
 , max(q_promoter_flag) as q_promoter_flag
-- Flipped Flags
 , min(q_ivr_bnc_3d_flipped_flag) as q_ivr_bnc_3d_flipped_flag             
 , min(q_closed_by_customer_flipped_flag) as q_closed_by_customer_flipped_flag
 , min(q_return_to_queue_flipped_flag) as q_return_to_queue_flipped_flag
 , min(q_web_bnc_3d_flipped_flag) as q_web_bnc_3d_flipped_flag
 , min(q_voice_bnc_3d_flipped_flag) as q_voice_bnc_3d_flipped_flag         
 , min(q_bnc_3d_flipped_flag) as q_bnc_3d_flipped_flag  
 , min(q_closed_by_timeout_flipped_flag) as q_closed_by_timeout_flipped_flag 
-- , min(q_chatbot_bnc_3d_flipped_flag) as q_chatbot_bnc_3d_flipped_flag  
 , min(q_chatlp_bnc_3d_flipped_flag) as q_chatlp_bnc_3d_flipped_flag 
 , min(q_detractor_flipped_flag) as q_detractor_flipped_flag 
 from chatlp_events_scored_v1
 where mtn is not null      
 group by month
 , mtn
 ) as a
 left join customer_datamart as b
 on a.mtn = b.mtn
 and a.month = b.month
;
