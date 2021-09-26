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
--															--
--	Author: Vanja Mileski (Delta Partners)					--
--	Description: Create the intents for live chat		    --
--															--
--	VERIZON QES PROJECT CODE (CONSUMER POSTPAID)			--
--															--
--	This script integrates the intents provided through     --
--  the business team and also through sentiment            --
--  modelling into the live chat datamart by prioritizing.  --
--															--
--	Input tables:											--
--		- ${QESPRDVW}.aa_a_assignment_v   			        --
--      - ${QESPRDVW}.cht_tp_score_v3                      --
--      - ${QESPRDVW}.cht_tp_score_v4                      --
--															--
--  Working tables:                                         --
--      - ${QESSTGTBLS}.chat_first_source_of_intents_v1 --
--      - ${QESSTGTBLS}.                                --
--          chat_second_source_of_intents_v1                --
--      - ${QESSTGTBLS}.chat_intents_v1                 --
--      - ${QESSTGTBLS}.chat_intents_v3                 --
--      - ${QESSTGTBLS}.chat_intents_v4                 --
--                                                          --
--	Output tables:											--
--      - ${QESSTGTBLS}.chat_intents_v2                 --
--      - ${QESSTGTBLS}.chat_intents                    --
--															--
--------------------------------------------------------------
--------------------------------------------------------------
-- Assigning a primary high level reason to an interaction
drop table if exists ${QESDUMMYBLS}.chatlp_intents;
create table chatlp_intents stored as orc tblproperties("orc.compress" = "SNAPPY") as
select mtn
,conversation_id
,chat_start_dt
,date_format(chat_start_dt,'YYYYMM') as month
,list_of_driver
,list_of_ctgry_driver
,size(list_of_ctgry_driver) as nbr_of_intents
,case
 when array_contains(list_of_driver,'Activate')           then 'Activate'
 when array_Contains(list_of_driver,'Disconnect')         then 'Disconnect'
 when array_Contains(list_of_driver,'Troubleshooting')    then 'Support'
 when array_Contains(list_of_driver,'Rewards')            then 'Rewards'
 when array_Contains(list_of_driver,'Bill')               then 'Billing'
 when array_Contains(list_of_driver,'Plan')               then 'Plan Change'
 when array_Contains(list_of_driver,'Upgrade')            then 'Sales and Upgrade'
 when array_Contains(list_of_driver,'Payment')            then 'Payment'
 when array_Contains(list_of_driver,'Learn')              then 'Learn'
 when array_Contains(list_of_driver,'Equipment')          then 'Device'
 when array_Contains(list_of_driver,'Account Maintenance') then 'Account Management'
 when array_Contains(list_of_driver,'International')      then 'Account Management'
 when array_Contains(list_of_driver,'Usage')              then 'Use'
 else null
 end as primary_high_level_reason
 from qes_prdstg_tbls.chatlp_events_original
;