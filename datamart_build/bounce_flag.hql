set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_tst_tbls;

use ${QESDUMMYBLS};
--------------------------------------------------------------
--------------------------------------------------------------
--                                       --
-- Author: Vladimir Burtaev (Delta Partners)          --
-- Description: Create call center features - typologies --
--                                    --
-- VERIZON QES PROJECT CODE (CONSUMER POSTPAID)         --
--                                       --
-- This script creates the logic required to create the    --
--  bounce flags for cross channels at an interaction       --
-- level                                           --
--  Input tables:                          --
--  - ${QESSTGTBLS}.voice_events_original               --
--                                                         --
--                                                          --
--  Output tables:                          --
-- - ${QESSTGTBLS}.bounce_final                        --
-- - ${QESSTGTBLS}.voice_typology_lookup             --
--                                    --
--------------------------------------------------------------
--------------------------------------------------------------
 -- Added new comments.
 -- TODO: Fix the chat ID & timestamp field names in step 1
 -- check logic for make sure an escalation from IVR to VOICE doesn't trigger bounce if they share the same ivr_call_id
 -- define logic for step 3 (reduction phase), whether we use inner joins or window functions
 -- Figure out how to include needed engineered fields in the step 1 while not using the <channel>_events_datamart tables
 -- [DJ] Change bounce time criteria to use interaction_end_ts for the source channel

 -- 300 secs
 DROP TABLE IF EXISTS ${QESDUMMYBLS}.bnc_all_events_prep;
 CREATE TABLE bnc_all_events_prep stored AS orc tblproperties ("orc.compress" = "SNAPPY") AS
 -- voice
 SELECT 'voice' AS channel
 ,mtn AS mtn
 ,'ivr_call_id' AS interaction_id_type
 ,ivr_call_id AS interaction_id
 ,min(unix_timestamp(call_segment_start_date_time)) AS interaction_start_ts
 FROM ${QESDUMMYBLS}.voice_events_original
 GROUP BY mtn
 ,ivr_call_id

 UNION ALL

 -- retail
 SELECT 'retail' AS channel
 ,mtn AS mtn
 ,'mtn|lower(store_id)|yr_mo_day' AS interaction_id_type
 ,interaction_id AS interaction_id
 ,min(unix_timestamp(visit_start_dttm)) AS interaction_start_ts
 FROM ${QESDUMMYBLS}.retail_events_original
 GROUP BY mtn
 ,interaction_id

 UNION ALL

 -- web
 SELECT 'web' AS channel
 ,mtn AS mtn
 ,'session_id' AS interaction_id_type
 ,session_id AS interaction_id
 ,min(unix_timestamp(min_timestamp)) AS interaction_start_ts
 FROM ${QESDUMMYBLS}.digital_web_events_original
 GROUP BY mtn
 ,session_id

 UNION ALL

 -- app
 SELECT 'app' AS channel
 ,mtn AS mtn
 ,'session_id' AS interaction_id_type
 ,session_id AS interaction_id
 ,min(unix_timestamp(min_timestamp)) AS interaction_start_ts
 FROM ${QESDUMMYBLS}.digital_app_events_original
 GROUP BY mtn
 ,session_id

 UNION ALL

 -- ivr
 SELECT 'ivr' AS channel
 ,mtn AS mtn
 ,'ivr_call_id' AS interaction_id_type
 ,ivr_call_id AS interaction_id
 ,min(unix_timestamp(ivr_call_start_dttm)) AS interaction_start_ts
 FROM ${QESDUMMYBLS}.ivr_events_original
 GROUP BY mtn
 ,ivr_call_id

 UNION ALL

 -- chatbot
 SELECT 'chatbot' AS channel
 ,mtn AS mtn
 ,'session_id' AS interaction_id_type
 ,session_id AS interaction_id
 ,min(unix_timestamp(session_start_dttm)) AS interaction_start_ts
 FROM ${QESDUMMYBLS}.chatbot_events_original
 GROUP BY mtn
 ,session_id

 UNION ALL

 -- Live Person
 SELECT 'chatlp' AS channel
 ,mtn AS mtn
 ,'conversation_id' AS interaction_id_type
 ,conversation_id AS interaction_id
 ,min(unix_timestamp(chat_start_ts)) AS interaction_start_ts
 FROM qes_prdstg_tbls.chatlp_events_original 
 GROUP BY mtn
 ,conversation_id;
 
 -------------------------------------------------------------
 ------               BOUNCE TIME MERGING               ------
 -------------------------------------------------------------
 ---------------------
 ---  PREPARATION  ---
 ---------------------
 -- this is requred as there is a lot of skew in the data and some mtns appear a lot of times
 -- offline analysis showed that mtns that ppear =<50 times in 6 months make up 95% of mtns, but only 66% of records
 DROP TABLE IF EXISTS ${QESDUMMYBLS}.bounce_mtn_skew_lookup;
 CREATE TABLE bounce_mtn_skew_lookup stored AS orc tblproperties ("orc.compress" = "SNAPPY") AS
 SELECT mtn
 ,appearance_freq
 ,CASE
 WHEN appearance_freq > 50
 THEN 1
 ELSE 0
 END AS has_skew
 ,CASE
 WHEN appearance_freq > 100
 THEN 1
 ELSE 0
 END AS has_skew_extreme
 FROM (
 SELECT mtn
 ,count(*) AS appearance_freq
 FROM ${QESDUMMYBLS}.bnc_all_events_prep
 GROUP BY mtn
 ) AS a;

 DROP TABLE IF EXISTS ${QESDUMMYBLS}.all_bounce_events;
 CREATE TABLE all_bounce_events stored AS orc tblproperties ("orc.compress" = "SNAPPY") AS
 SELECT a.channel
 ,a.mtn
 ,a.interaction_id_type
 ,a.interaction_id
 ,a.interaction_start_ts
 ,b.channel AS bnc_channel
 ,b.interaction_id AS bnc_interaction_id
 ,b.interaction_start_ts AS bnc_interaction_start_ts
 ,(b.interaction_start_ts - a.interaction_start_ts) / 3600 AS hrs_to_bnc
 FROM (
 -- Getting rid of extremely high frequency mtn records
 SELECT mtn
 FROM ${QESDUMMYBLS}.bounce_mtn_skew_lookup
 WHERE appearance_freq < 2000
 ) AS c
 INNER JOIN (
 -- Table that serves as the source channel
 SELECT channel
 ,mtn
 ,interaction_id_type
 ,interaction_id
 ,interaction_start_ts
 FROM ${QESDUMMYBLS}.bnc_all_events_prep
 ) AS a ON a.mtn = c.mtn
 -- Table that serves as the bounce channel
 INNER JOIN ${QESDUMMYBLS}.bnc_all_events_prep AS b ON c.mtn = b.mtn
 -- and b.interaction_start_ts
 --  between
 --  a.interaction_start_ts and (a.interaction_start_ts + 259200)
 WHERE a.interaction_id <> b.interaction_id
 AND b.interaction_start_ts BETWEEN a.interaction_start_ts
 AND (a.interaction_start_ts + 259200);

 -- additional fields that DJ had: exception_channel_id, end_ts, high_level_reason, intensity, service_intent, sales_intent, channel_group
 -- Step 3: If multiple bounces to the same channel, pick only the first instance in the 3 day time period
 DROP TABLE IF EXISTS ${QESDUMMYBLS}.bounce_per_channel;
 CREATE TABLE bounce_per_channel stored AS orc tblproperties ("orc.compress" = "SNAPPY") AS
 WITH correct_base AS (
 SELECT channel
 ,mtn
 ,interaction_id_type
 ,interaction_id
 ,interaction_start_ts
 ,bnc_channel
 ,bnc_interaction_id
 ,bnc_interaction_start_ts
 ,hrs_to_bnc
 FROM ${QESDUMMYBLS}.all_bounce_events
 )

 -- where   -- IVR to IVR should not exist as a bounce
 --         case when channel in ('ivr') and bnc_channel in ('ivr') then 1 else 0 end = 0
 --         and
 --         -- bouncing IVR to chat within the hour is by design of the system
 --         case when channel in ('ivr') and bnc_channel in ('chatbot') and hrs_to_bnc <= 1 then 1 else 0 end = 0
 SELECT channel
 ,mtn
 ,interaction_id_type
 ,interaction_id
 ,interaction_start_ts
 ,bnc_channel
 ,bnc_interaction_id
 ,bnc_interaction_start_ts
 ,hrs_to_bnc
 FROM (
 SELECT channel
 ,mtn
 ,interaction_id_type
 ,interaction_id
 ,interaction_start_ts
 ,bnc_channel
 ,bnc_interaction_id
 ,bnc_interaction_start_ts
 ,hrs_to_bnc
 ,row_number() OVER (
 PARTITION BY channel
 ,mtn
 ,interaction_id_type
 ,interaction_id
 ,interaction_start_ts
 ,bnc_channel ORDER BY bnc_interaction_start_ts ASC
 ) AS row_num
 FROM correct_base
 ) AS ordered_base
 WHERE row_num = 1;

 INSERT OVERWRITE DIRECTORY 'qes_oozie/status_check1'
 SELECT 0
 FROM ${QESDUMMYBLS}.customer_datamart limit 1;

 -- Step 4: Select one bounce per interaction - earliest
 DROP TABLE IF EXISTS ${QESDUMMYBLS}.bounce_final;
 CREATE TABLE bounce_final stored AS orc tblproperties ("orc.compress" = "SNAPPY") AS
 SELECT channel
 ,mtn
 ,interaction_id_type
 ,interaction_id
 ,interaction_start_ts
 ,bnc_channel
 ,bnc_interaction_id
 ,bnc_interaction_start_ts
 ,hrs_to_bnc
 FROM (
 SELECT channel
 ,mtn
 ,interaction_id_type
 ,interaction_id
 ,interaction_start_ts
 ,bnc_channel
 ,bnc_interaction_id
 ,bnc_interaction_start_ts
 ,hrs_to_bnc
 ,row_number() OVER (
 PARTITION BY channel
 ,mtn
 ,interaction_id_type
 ,interaction_id
 ,interaction_start_ts ORDER BY bnc_interaction_start_ts ASC
 ) AS row_num
 FROM ${QESDUMMYBLS}.bounce_per_channel
 ) AS a
 WHERE row_num = 1;

-- INSERT OVERWRITE DIRECTORY 'qes_oozie/status_check1'
-- SELECT count(*)
-- FROM ${QESDUMMYBLS}.bounce_final;
