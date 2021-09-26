set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_dummy_tbls;

use ${QESDUMMYBLS};
-----------------------------------------------------------------------
----------------------------- Score Build -----------------------------
-----------------------------------------------------------------------
-- Step 1: Transpose weight table for cross join
drop table if exists ${QESDUMMYBLS}.chatlp_model_weights_final;
create table chatlp_model_weights_final stored as orc tblproperties('orc.compress' = 'SNAPPY') as
select ivr_bnc.weight as ivr_bnc_weight
, a_bnc.weight as a_bnc_weight  
, v_bnc.weight as v_bnc_weight
, w_bnc.weight as w_bnc_weight 
-- , all_bnc.weight as all_bnc_weight
-- , chatbot_bnc.weight as chatbot_bnc_weight
-- , chatlp_bnc.weight as chatlp_bnc_weight
, r_bnc.weight as r_bnc_weight
, arr_avg.weight as arr_avg_weight
-- , arr_sum.weight as arr_sum_weight
, r2q.weight as r2q_flipped_weight
, close_by_agent.weight as close_by_agent_weight
, close_by_cust.weight as close_by_cust_weight
, close_by_timeout.weight as close_by_timeout_weight
, sale.weight as sale_conversion_weight
, t_msg.weight as t_msg_weight
, agent.weight as agent_weight
-- , art_avg.weight as art_avg_weight
--, arr_q.weight as arr_q_weight
--, chat_connect.weight as chat_connect_weight
-- , first_avg.weight as first_avg_weight
-- , detractor.weight as detractor_weight
from  (select weight from chatlp_model_weights where metric_name = 'q_ivr_bnc_3d_flipped_flag') as ivr_bnc
 cross join (select weight from chatlp_model_weights where metric_name = 'q_app_bnc_3d_flag') as a_bnc
 cross join (select weight from chatlp_model_weights where metric_name = 'q_voice_bnc_3d_flipped_flag') as v_bnc
 cross join (select weight from chatlp_model_weights where metric_name = 'q_web_bnc_3d_flipped_flag') as w_bnc
-- cross join (select weight from chatlp_model_weights where metric_name = 'q_bnc_3d_flipped_flag') as all_bnc
-- cross join (select weight from chatlp_model_weights where metric_name = 'q_chatbot_bnc_3d_flag') as chatbot_bnc 
-- cross join (select weight from chatlp_model_weights where metric_name = 'q_chatlp_bnc_3d_flipped_flag') as chatlp_bnc
 cross join (select weight from chatlp_model_weights where metric_name = 'q_retail_bnc_3d_flag') as r_bnc
 cross join (select weight from chatlp_model_weights where metric_name = 'q_arr_resptime_avg') as arr_avg
-- cross join (select weight from chatlp_model_weights where metric_name = 'q_arr_resptime_avg') as arr_sum 
 cross join (select weight from chatlp_model_weights where metric_name = 'q_return_to_queue_flipped_flag') as r2q
 cross join (select weight from chatlp_model_weights where metric_name = 'q_closed_by_agent_flag') as close_by_agent
 cross join (select weight from chatlp_model_weights where metric_name = 'q_closed_by_customer_flipped_flag') as close_by_cust
 cross join (select weight from chatlp_model_weights where metric_name = 'q_closed_by_timeout_flipped_flag') as close_by_timeout
 cross join (select weight from chatlp_model_weights where metric_name = 'q_sales_conversion_flag') as sale
 cross join (select weight from chatlp_model_weights where metric_name = 'q_total_msg') as t_msg
 cross join (select weight from chatlp_model_weights where metric_name = 'q_agent_score') as agent
-- cross join (select weight from chatlp_model_weights where metric_name = 'q_cust_first_resptime_avg') as first_avg
-- cross join (select weight from chatlp_model_weights where metric_name = 'q_art_resptime_avg') as art_avg
-- cross join (select weight from chatlp_model_weights where metric_name = 'q_arr_q') as arr_q
-- cross join (select weight from chatlp_model_weights where metric_name = 'q_chat_connection_score') as chat_connect
-- cross join (select weight from chatlp_model_weights where metric_name = 'q_detractor_flipped_flag') as detractor
;

-- Step 2: Create a table with the weights applied to each row (n_ = numerator value, w_ = pure weight)
drop table if exists ${QESDUMMYBLS}.chatlp_events_scored_v2;
create table chatlp_events_scored_v2 stored as orc tblproperties('orc.compress' = 'SNAPPY') as 
select  a.*
---------------------- BOUNCE FLAGS -------------------------
-- Voice Bounce Flag (flipped)
, case 
when q_voice_bnc_3d_flipped_flag in (0,1)
 then v_bnc_weight 
 else 0 end as w_voice_bnc_3d_flipped_flag
, case 
 when q_voice_bnc_3d_flipped_flag = 0 then v_bnc_weight * 1
 when q_voice_bnc_3d_flipped_flag = 1 then v_bnc_weight * 5
 else 0 end as n_voice_bnc_3d_flipped_flag
-- Web Bounce Flag (flipped)
, case 
 when q_web_bnc_3d_flipped_flag in (0,1)
 then w_bnc_weight
 else 0 end as w_web_bnc_3d_flipped_flag
, case 
 when q_web_bnc_3d_flipped_flag = 0 then w_bnc_weight * 1
 when q_web_bnc_3d_flipped_flag = 1 then w_bnc_weight * 5
 else 0 end as n_web_bnc_3d_flipped_flag
-- IVR Bounce Flag (flipped)
, case 
 when q_ivr_bnc_3d_flipped_flag in (0,1)
 then ivr_bnc_weight
 else 0 end as w_ivr_bnc_3d_flipped_flag
, case 
 when q_ivr_bnc_3d_flipped_flag = 0 then ivr_bnc_weight * 1
 when q_ivr_bnc_3d_flipped_flag = 1 then ivr_bnc_weight * 5
 else 0 end as n_ivr_bnc_3d_flipped_flag       
-- All Bounce Flag (flipped)
-- , case 
--  when q_bnc_3d_flipped_flag in (0,1)
--  then all_bnc_weight
--  else 0 end as w_all_bnc_3d_flipped_flag       
--  , case 
--  when q_bnc_3d_flipped_flag = 0 then all_bnc_weight * 1
--  when q_bnc_3d_flipped_flag = 1 then all_bnc_weight * 5
--  else 0 end as n_all_bnc_3d_flipped_flag
-- Chatbot Bounce Flag 
-- , case 
--  when q_chatbot_bnc_3d_flag in (0,1)
--  then chatbot_bnc_weight
--  else 0 end as w_chatbot_bnc_3d_flag
-- , case 
-- when q_chatbot_bnc_3d_flag = 0 then chatbot_bnc_weight * 1
--  when q_chatbot_bnc_3d_flag = 1 then chatbot_bnc_weight * 5
--  else 0 end as n_chatbot_bnc_3d_flag
-- Chatlp Bounce Flag (flipped)
-- , case 
--  when q_chatlp_bnc_3d_flipped_flag in (0,1)
--  then chatlp_bnc_weight
--  else 0 end as w_chatlp_bnc_3d_flipped_flag
-- , case 
--  when q_chatlp_bnc_3d_flipped_flag = 0 then chatlp_bnc_weight * 1
--  when q_chatlp_bnc_3d_flipped_flag = 1 then chatlp_bnc_weight * 5
--  else 0 end as n_chatlp_bnc_3d_flipped_flag
-- App Bounce Flag  
, case 
 when q_app_bnc_3d_flag in (0,1)
 then a_bnc_weight
 else 0 end as w_app_bnc_3d_flag       
, case 
 when q_app_bnc_3d_flag = 0 then a_bnc_weight * 1
 when q_app_bnc_3d_flag = 1 then a_bnc_weight * 5
 else 0 end as n_app_bnc_3d_flag
-- Retail Bounce Flag  
, case 
 when q_retail_bnc_3d_flag in (0,1)
 then r_bnc_weight
 else 0 end as w_retail_bnc_3d_flag       
, case 
 when q_retail_bnc_3d_flag = 0 then r_bnc_weight * 1
 when q_retail_bnc_3d_flag = 1 then r_bnc_weight * 5
 else 0 end as n_retail_bnc_3d_flag
---------------------- IMBALANCED FLAGS -------------------------
-- Conversion
, case 
 when q_sales_conversion_flag in (1)
 then sale_conversion_weight  
 else 0 end as w_sales_conversion_flag
, case 
 when q_sales_conversion_flag = 1 then sale_conversion_weight * 5
 else 0 end as n_sales_conversion_flag
-- Close by Customer
, case 
 when q_closed_by_customer_flipped_flag in (1)
 then close_by_cust_weight
 else 0 end as w_closed_by_customer_flag
, case 
 when q_closed_by_customer_flipped_flag = 1 then close_by_cust_weight * 5
 else 0 end as n_closed_by_customer_flag
-- Close by Agent
, case 
 when q_closed_by_agent_flag in (1)
 then close_by_agent_weight  
 else 0 end as w_close_by_agent_flag
, case 
 when q_closed_by_agent_flag = 1 then close_by_agent_weight * 5
 else 0 end as n_close_by_agent_flag
-- Close by Timeout
, case 
 when q_closed_by_timeout_flipped_flag in (1)
 then close_by_timeout_weight
 else 0 end as w_closed_by_timeout_flipped_flag
, case 
 when q_closed_by_timeout_flipped_flag = 1 then close_by_timeout_weight * 5
 else 0 end as n_closed_by_timeout_flipped_flag
-- Return to queue
, case 
 when q_return_to_queue_flipped_flag in (1)
 then r2q_flipped_weight  
 else 0 end as w_return_to_queue_flipped_flag
, case 
 when q_return_to_queue_flipped_flag = 1 then r2q_flipped_weight * 5
 else 0 end as n_return_to_queue_flipped_flag
-- Detractor_flipped
-- , case 
--  when q_detractor_flipped_flag in (1)
-- then detractor_weight  
-- else 0 end as w_detractor_flipped_flag
--, case 
-- when q_detractor_flipped_flag = 1 then detractor_weight * 5
-- else 0 end as n_detractor_flipped_flag
 
---------------------- CONTINUOUS SCORES ------------------------
-- Avg Rep Response in Seconds
, case 
  when q_arr_resptime_avg >= 1 
  then arr_avg_weight 
  else 0 end as w_arr_resptime_avg
, case 
  when q_arr_resptime_avg >= 1 
  then arr_avg_weight * q_arr_resptime_avg
  else 0 end as n_arr_resptime_avg 
-- Sum Rep Response in Seconds
-- , case 
--   when q_arr_resptime_sum >= 1 
--   then arr_sum_weight 
--   else 0 end as w_arr_resptime_sum
-- , case 
--   when q_arr_resptime_sum >= 1 
--   then arr_sum_weight * q_arr_resptime_sum
--   else 0 end as n_arr_resptime_sum 
-- Total message per conversaton
, case 
 when q_total_msg >= 1 
 then t_msg_weight 
 else 0 end as w_total_msg
, case 
 when q_total_msg >= 1 
 then t_msg_weight * q_total_msg     
 else 0 end as n_total_msg           
-- Agent Score
, case 
 when q_agent_score >= 1 
 then agent_weight 
 else 0 end as w_agent
, case 
 when q_agent_score >= 1 
 then agent_weight * q_agent_score     
 else 0 end as n_agent 
-- Cust First Resptime Avg Score
-- , case 
--  when q_cust_first_resptime_avg >= 1 
--  then first_avg_weight 
--  else 0 end as w_cust_first_resptime_avg 
-- , case 
--  when q_cust_first_resptime_avg >= 1 
--  then first_avg_weight * q_cust_first_resptime_avg     
--  else 0 end as n_cust_first_resptime_avg 
-- Avg art Response in Seconds
-- , case 
-- when q_art_resptime_avg >= 1 
-- then art_avg_weight 
-- else 0 end as w_art_resptime_avg
-- , case 
-- when q_art_resptime_avg >= 1 
-- then art_avg_weight * q_art_resptime_avg
-- else 0 end as n_art_resptime_avg 
-- arr_q in Seconds
-- , case 
--  when q_arr_q >= 1 
--  then arr_q_weight 
--  else 0 end as w_arr_q
--  , case 
--  when q_arr_q >= 1 
--  then arr_q_weight * q_arr_q
--  else 0 end as n_arr_q 
-- q_chat_connection_score  
-- , case 
-- when q_chat_connection_score >= 1 
-- then chat_connect_weight 
-- else 0 end as w_chat_connect
-- , case 
-- when q_chat_connection_score >= 1 
-- then chat_connect_weight * q_chat_connection_score
-- else 0 end as n_chat_connect 
 from chatlp_events_scored_v1 as a
 cross join chatlp_model_weights_final as b
;

-- Step 3: Calculate the denominator of weights for each row
drop table if exists ${QESDUMMYBLS}.chatlp_events_scored_v3;
create table chatlp_events_scored_v3 stored as orc tblproperties('orc.compress' = 'SNAPPY') as 
select a.*
 , w_voice_bnc_3d_flipped_flag 
 + w_web_bnc_3d_flipped_flag 
 + w_ivr_bnc_3d_flipped_flag  
-- + w_all_bnc_3d_flipped_flag 
-- + w_chatbot_bnc_3d_flag 
-- + w_chatlp_bnc_3d_flipped_flag
 + w_app_bnc_3d_flag 
 + w_retail_bnc_3d_flag 
 + w_sales_conversion_flag 
 + w_closed_by_customer_flag 
 + w_closed_by_timeout_flipped_flag
 + w_close_by_agent_flag 
 + w_return_to_queue_flipped_flag 
-- + w_detractor_flipped_flag
 + w_arr_resptime_avg 
-- + w_arr_resptime_sum
 + w_total_msg  
 + w_agent
-- + w_cust_first_resptime_avg
-- + w_art_resptime_avg
-- + w_arr_q
-- + w_chat_connect
 as weight_denominator
 , n_voice_bnc_3d_flipped_flag 
 + n_web_bnc_3d_flipped_flag
 + n_ivr_bnc_3d_flipped_flag
-- + n_all_bnc_3d_flipped_flag
-- + n_chatbot_bnc_3d_flag 
-- + n_chatlp_bnc_3d_flipped_flag 
 + n_app_bnc_3d_flag
 + n_retail_bnc_3d_flag
 + n_sales_conversion_flag
 + n_closed_by_customer_flag 
 + n_closed_by_timeout_flipped_flag
 + n_close_by_agent_flag
 + n_return_to_queue_flipped_flag 
-- + n_detractor_flipped_flag
 + n_arr_resptime_avg
-- + n_arr_resptime_sum
 + n_total_msg
 + n_agent 
-- + n_cust_first_resptime_avg 
-- + n_art_resptime_avg
-- + n_arr_q
-- + n_chat_connect
 as score_numerator
from chatlp_events_scored_v2 as a
;

-- Step 4: Apply the weights to the
drop table if exists ${QESDUMMYBLS}.chatlp_events_scored;
create table chatlp_events_scored stored as orc tblproperties('orc.compress' = 'SNAPPY') as 
select a.*
, round(score_numerator / weight_denominator, 2) as quality_score
from chatlp_events_scored_v3 as a
;

-- Step 5: Aggregate to to monthly customer snapshot
drop table if exists ${QESDUMMYBLS}.chatlp_customer_scored;
create table chatlp_customer_scored stored as orc tblproperties("orc.compress" = "SNAPPY") as
select month
, mtn
, count(*) as event_cnt
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
, max(q_sales_conversion_flag) as q_sales_conversion_flag
, max(q_closed_by_agent_flag) as q_closed_by_agent_flag
, max(q_promoter_flag) as q_promoter_flag
-- Flipped Flags
, min(q_ivr_bnc_3d_flipped_flag) as q_ivr_bnc_3d_flipped_flag
, min(q_closed_by_customer_flipped_flag) as q_closed_by_customer_flipped_flag
, min(q_closed_by_timeout_flipped_flag) as q_closed_by_timeout_flipped_flag
, min(q_return_to_queue_flipped_flag) as q_return_to_queue_flipped_flag
, min(q_web_bnc_3d_flipped_flag) as q_web_bnc_3d_flipped_flag  
, min(q_voice_bnc_3d_flipped_flag) as q_voice_bnc_3d_flipped_flag  
, min(q_bnc_3d_flipped_flag) as q_bnc_3d_flipped_flag  
-- , min(q_chatbot_bnc_3d_flipped_flag) as q_chatbot_bnc_3d_flipped_flag
, min(q_chatlp_bnc_3d_flipped_flag) as q_chatlp_bnc_3d_flipped_flag
, min(q_detractor_flipped_flag) as q_detractor_flipped_flag
, avg(quality_score) as avg_quality_score
, min(quality_score) as min_quality_score
from chatlp_events_scored      
group by month, mtn
;

--- modify after refresh data -------
----------------------------------------------------------------------
----------------------------- Validation -----------------------------
----------------------------------------------------------------------
- Check that counts are aligned.

use deltap_prd_qmtbls;
select count(*), 'chatlp_events_original' from qes_prdstg_tbls.chatlp_events_original
union all
select count(*), 'chatlp_events_datamart' from chatlp_events_datamart
union all
select count(*), 'chatlp_events_scored_v1' from chatlp_events_scored_v1
union all
select count(*), 'chatlp_events_scored' from chatlp_events_scored
union all
select count(*), 'chatlp_customer_scored_v1' from chatlp_customer_scored_v1
union all
select count(*), 'chatlp_customer_scored' from chatlp_customer_scored
;

create temporary macro nullif(s string, p string) if(s = p, null, s);

-- Check the churn curves
select      floor(a.quality_score)                          as quality_score
            , count(*)                                      as total_records
            , sum(b.churn_vol_m1_flag)                      as churners
            , 'event-level snapshot'                        as granularity
            , '202004 to 202006'                            as timewindow
            , 'app'                                         as platform
from        chatlp_events_scored as a
            left join customer_datamart as b
            on  a.mtn = b.mtn
                and a.month = b.month
where       a.month between 202004 and 202006 and nullif(a.mtn,'') is not null
group by    floor(a.quality_score)
union all
select      floor(a.avg_quality_score)                      as quality_score
            , count(*)                                      as total_records
            , sum(b.churn_vol_m1_flag)                      as churners
            , 'AVERAGE Monthly quality QeS by Customer'     as granularity
            , '201810 to 201903'                            as timewindow
            , 'app'                                         as platform
from        deltap_prd_qmtbls.chat_customer_scored   as a
            left join deltap_prd_qmtbls.customer_datamart   as b
            on  a.msisdn = b.msisdn
                and a.month = b.month
where       a.month between 201810 and 201903 and nullif(a.msisdn,'') is not null
group by    floor(a.avg_quality_score)
union all
select      floor(a.min_quality_score)                      as quality_score
            , count(*)                                      as total_records
            , sum(b.churn_vol_m1_flag)                      as churners
            , 'MINIMUM Monthly quality QeS by Customer'     as granularity
            , '201810 to 201903'                            as timewindow
            , 'app'                                         as platform
from        deltap_prd_qmtbls.chat_customer_scored   as a
            left join deltap_prd_qmtbls.customer_datamart   as b
            on  a.msisdn = b.msisdn
                and a.month = b.month
where       a.month between 201810 and 201903 and nullif(a.msisdn,'') is not null
group by    floor(a.min_quality_score)
;
