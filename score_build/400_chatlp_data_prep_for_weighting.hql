set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_dummy_tbls;

use ${QESDUMMYBLS};

drop table if exists ${QESDUMMYBLS}.chatlp_customer_overall_v1;
create table chatlp_customer_overall_v1 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select  
a.mtn
, a.month
, a.avg_quality_score
, a.min_quality_score
, b.q_nature_Score
from ${QESDUMMYBLS}.chatlp_customer_scored as a
inner join ${QESDUMMYBLS}.chatlp_nature_score_datamart as b
on  a.mtn = b.mtn
and a.month = b.month;

drop table if exists ${QESDUMMYBLS}.chatlp_customer_overall_v2;
create table chatlp_customer_overall_v2 stored as orc tblproperties("orc.compress" = "SNAPPY") as 
select a.*
, coalesce(b.churn_vol_m1_flag,0) as churn_vol_m1_flag 
from ${QESDUMMYBLS}.chatlp_customer_overall_v1 as a 
left join ${QESDUMMYBLS}.customer_datamart as b 
on a.mtn = b.mtn 
and a.month = b.month 
;