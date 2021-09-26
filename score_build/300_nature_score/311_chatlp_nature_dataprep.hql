set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_dummy_tbls;

use ${QESDUMMYBLS};

drop table if exists ${QESDUMMYBLS}.chatlp_nature_score_aggregate_v1;
create table chatlp_nature_score_aggregate_v1 stored as orc tblproperties("orc.compress" = "SNAPPY") as
select round(a.nature_score*100,0) as nature_score
, count(*) as events
, sum(b.churn_vol_m1_flag) as churn_m1
, sum(coalesce(churn_vol_m1_flag,0)) / count(*) as churn_rate_m1
from ${QESDUMMYBLS}.chatlp_nature_scores as a
left join ${QESDUMMYBLS}.customer_datamart as b
on  a.mtn = b.mtn 
and a.month = b.month
where a.month between 202005 and 202009
group by round(a.nature_score*100,0)
;
