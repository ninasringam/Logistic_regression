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
--               --
-- Author: Daniel Julien (Delta Partners)     --
-- Description: Create the live chat bounce features  --
--               --
-- VERIZON QES PROJECT CODE (CONSUMER POSTPAID)   --
--               --
-- This script integrates the bounce flag into the         --
--  live chat datamart.                                     --
--               --
-- Input tables:           --
--  - ${QESSTGTBLS}.bounce_final              --
--      - ${QESSTGTBLS}.bounce_per_channel              --
--               --
--  Working tables:                                         --
--      - ${QESSTGTBLS}.chat_bounce_to_all_v1           --
--                                                          --
-- Output tables:           --
--      - ${QESSTGTBLS}.chat_bounce_v1                  --
--               --
--------------------------------------------------------------
--------------------------------------------------------------

-- STEP 1: Extract all events which had a bounce, regardless of the bounced channel
DROP TABLE IF EXISTS ${QESDUMMYBLS}.chatlp_bounce_to_all_v1;
CREATE TABLE chatlp_bounce_to_all_v1 stored AS orc tblproperties ("orc.compress" = "SNAPPY") AS
SELECT mtn
,interaction_id
,bnc_channel
,bnc_interaction_id
,hrs_to_bnc
FROM ${QESDUMMYBLS}.bounce_final
WHERE lower(channel) = 'chatlp';

-- STEP 2: Add channel-specific bounce information
DROP TABLE IF EXISTS ${QESDUMMYBLS}.chatlp_bounce_v1;
CREATE TABLE chatlp_bounce_v1 stored AS orc tblproperties ("orc.compress" = "SNAPPY") AS
SELECT x.mtn
,x.interaction_id
,x.bnc_channel
,x.bnc_interaction_id
,x.hrs_to_bnc
,r.bnc_channel AS retail_bnc_channel
,r.bnc_interaction_id AS retail_bnc_interaction_id
,r.hrs_to_bnc AS retail_hrs_to_bnc
,v.bnc_channel AS voice_bnc_channel
,v.bnc_interaction_id AS voice_bnc_interaction_id
,v.hrs_to_bnc AS voice_hrs_to_bnc
,i.bnc_channel AS ivr_bnc_channel
,i.bnc_interaction_id AS ivr_bnc_interaction_id
,i.hrs_to_bnc AS ivr_hrs_to_bnc
,w.bnc_channel AS web_bnc_channel
,w.bnc_interaction_id AS web_bnc_interaction_id
,w.hrs_to_bnc AS web_hrs_to_bnc
,a.bnc_channel AS app_bnc_channel
,a.bnc_interaction_id AS app_bnc_interaction_id
,a.hrs_to_bnc AS app_hrs_to_bnc
,c.bnc_channel AS chatlp_bnc_channel
,c.bnc_interaction_id AS chatlp_bnc_interaction_id
,c.hrs_to_bnc AS chatlp_hrs_to_bnc
,cb.bnc_channel AS chatbot_bnc_channel
,cb.bnc_interaction_id AS chatbot_bnc_interaction_id
,cb.hrs_to_bnc AS chatbot_hrs_to_bnc
FROM ${QESDUMMYBLS}.chatlp_bounce_to_all_v1 AS x
LEFT JOIN (
-- bounce to retail
SELECT *
FROM ${QESDUMMYBLS}.bounce_per_channel
WHERE lower(channel) = 'chatlp'
AND lower(bnc_channel) = 'retail'
) AS r ON x.interaction_id = r.interaction_id
AND x.mtn = r.mtn
LEFT JOIN (
-- bounce to call center
SELECT *
FROM ${QESDUMMYBLS}.bounce_per_channel
WHERE lower(channel) = 'chatlp'
AND lower(bnc_channel) = 'voice'
) AS v ON x.interaction_id = v.interaction_id
AND x.mtn = v.mtn
LEFT JOIN (
-- bounce to IVR
SELECT *
FROM ${QESDUMMYBLS}.bounce_per_channel
WHERE lower(channel) = 'chatlp'
AND lower(bnc_channel) = 'ivr'
) AS i ON x.interaction_id = i.interaction_id
AND x.mtn = i.mtn
LEFT JOIN (
-- bounce to web
SELECT *
FROM ${QESDUMMYBLS}.bounce_per_channel
WHERE lower(channel) = 'chatlp'
AND lower(bnc_channel) = 'web'
) AS w ON x.interaction_id = w.interaction_id
AND x.mtn = w.mtn
LEFT JOIN (
-- bounce to app
SELECT *
FROM ${QESDUMMYBLS}.bounce_per_channel
WHERE lower(channel) = 'chatlp'
AND lower(bnc_channel) = 'app'
) AS a ON x.interaction_id = a.interaction_id
AND x.mtn = a.mtn
LEFT JOIN (
-- bounce to chatlp again
SELECT *
FROM ${QESDUMMYBLS}.bounce_per_channel
WHERE lower(channel) = 'chatlp'
AND lower(bnc_channel) = 'chatlp'
) AS c ON x.interaction_id = c.interaction_id
AND x.mtn = c.mtn
LEFT JOIN (
-- bounce to chatbot
SELECT *
FROM ${QESDUMMYBLS}.bounce_per_channel
WHERE lower(channel) = 'chatlp'
AND lower(bnc_channel) = 'chatbot'
) AS cb ON x.interaction_id = cb.interaction_id
AND x.mtn = cb.mtn;
