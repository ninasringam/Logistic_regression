set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_tst_tbls;

use ${QESDUMMYBLS};

create temporary macro nullif(s string, p string) if(s = p, null, s);

-- Bounce Flags
drop table if exists ${QESDUMMYBLS}.chatlp_events_datamart_v3;
create table chatlp_events_datamart_v3 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select a.*
-- Overall Bounce...
 ,d.bnc_channel
 ,d.bnc_interaction_id
 ,d.hrs_to_bnc
 ,case when nullif(d.bnc_channel,'') is not null then 1 else 0 end as bnc_3d_flag
-- Channel-specific Bounce...
 ,d.retail_bnc_channel
 ,d.retail_bnc_interaction_id
 ,d.retail_hrs_to_bnc
 ,d.voice_bnc_channel
 ,d.voice_bnc_interaction_id
 ,d.voice_hrs_to_bnc
 ,d.ivr_bnc_channel
 ,d.ivr_bnc_interaction_id
 ,d.ivr_hrs_to_bnc
 ,d.web_bnc_channel
 ,d.web_bnc_interaction_id
 ,d.web_hrs_to_bnc
 ,d.app_bnc_channel
 ,d.app_bnc_interaction_id
 ,d.app_hrs_to_bnc
 ,d.chatlp_bnc_channel
 ,d.chatlp_bnc_interaction_id
 ,d.chatlp_hrs_to_bnc
 ,d.chatbot_bnc_channel
 ,d.chatbot_bnc_interaction_id
 ,d.chatbot_hrs_to_bnc
 ,case when lower(d.retail_bnc_channel)  = 'retail'  then 1 else 0 end as retail_bnc_3d_flag
 ,case when lower(d.voice_bnc_channel)   = 'voice'   then 1 else 0 end as voice_bnc_3d_flag
 ,case when lower(d.ivr_bnc_channel)     = 'ivr'     then 1 else 0 end as ivr_bnc_3d_flag
 ,case when lower(d.web_bnc_channel)     = 'web'     then 1 else 0 end as web_bnc_3d_flag
 ,case when lower(d.app_bnc_channel)     = 'app'     then 1 else 0 end as app_bnc_3d_flag
 ,case when lower(d.chatlp_bnc_channel)  = 'chatlp'  then 1 else 0 end as chatlp_bnc_3d_flag
 ,case when lower(d.chatbot_bnc_channel) = 'chatbot' then 1 else 0 end as chatbot_bnc_3d_flag
 from qes_prdstg_tbls.chatlp_events_original as a
 left join chatlp_bounce_v1 as d
 on a.conversation_id = d.interaction_id
 and a.mtn = d.mtn
;

-- Intents & Typologies
drop table if exists ${QESDUMMYBLS}.chatlp_events_datamart_v4;
create table chatlp_events_datamart_v4 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select a.*
 ,e.nbr_of_intents
 ,coalesce(nullif(e.primary_high_level_reason,''),'None') as primary_high_level_reason
 from chatlp_events_datamart_v3 as a
 left join chatlp_intents as e
 on cast(a.conversation_id as string) = cast(e.conversation_id as string)
 and a.mtn = e.mtn
;

-- Agent Scores
drop table if exists ${QESDUMMYBLS}.chatlp_events_datamart_v5;
create table chatlp_events_datamart_v5 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select a.*
 ,f.agent_score
 from chatlp_events_datamart_v4 as a
 left join chatlp_agent_quality_v1 as f
 on a.conversation_id = f.conversation_id
 and a.mtn = f.mtn
;

-- Final Events Datamart
drop table if exists ${QESDUMMYBLS}.chatlp_events_datamart;
create table chatlp_events_datamart stored as orc tblproperties("orc.compress" = "SNAPPY") as
select -- ORIGINAL FEATURES: Identifiers
mtn
, conversation_id
, chat_start_ts 
, chat_start_dt          
, month
-- ORIGINAL FEATURES: Interaction Time
, cust_first_resptime_max
, cust_first_resptime_avg
, art_resptime_avg
, art_resptime_sum
, arr_q 
, arr_resptime_avg
, arr_resptime_sum
, duration
, total_msg
, total_rep 
, chat_connection_score
-- Success Measures
, agent_transfer_flag
, return_to_queue_flag
, closed_by_agent_flag
, closed_by_customer_flag
, closed_by_timeout_flag
, sales_conversion_flag
-- The following features are delta-engineered bounce flags
, bnc_3d_flag
, retail_bnc_3d_flag
, voice_bnc_3d_flag
, ivr_bnc_3d_flag
, web_bnc_3d_flag
, app_bnc_3d_flag
, chatlp_bnc_3d_flag
, chatbot_bnc_3d_flag
-- The following features are bounce-related datapoints
, bnc_channel
, bnc_interaction_id
, hrs_to_bnc
, retail_bnc_channel
, retail_bnc_interaction_id
, retail_hrs_to_bnc
, voice_bnc_channel
, voice_bnc_interaction_id
, voice_hrs_to_bnc
, ivr_bnc_channel
, ivr_bnc_interaction_id
, ivr_hrs_to_bnc
, web_bnc_channel
, web_bnc_interaction_id
, web_hrs_to_bnc
, app_bnc_channel
, app_bnc_interaction_id
, app_hrs_to_bnc
, chatlp_bnc_channel
, chatlp_bnc_interaction_id
, chatlp_hrs_to_bnc
, chatbot_bnc_channel
, chatbot_bnc_interaction_id
, chatbot_hrs_to_bnc
-- Columns from intent table
, list_of_ctgry_driver    
, nbr_of_intents
, list_of_driver          
, primary_high_level_reason
-- Feedback
, nps_avg 
, csat_avg 
, custom_avg  
, case
when nps_avg in (9,10)
and csat_avg in (9,10)
then 'Y'
else 'N'
end as promoter_flag
,case
when nps_avg in (0,1,2,3,4,5,6)
or csat_avg in (0,1,2,3,4,5,6)
then 'Y'
else 'N'
end as detractor_flag
-- Agents & Call Center Information
, list_of_transfer_agent
, size(list_of_transfer_agent) as agent_id_cnt
, case when size(list_of_transfer_agent) > 1 then 1 else 0 end as multiple_agent_flag
, agent_score
from chatlp_events_datamart_v5
;