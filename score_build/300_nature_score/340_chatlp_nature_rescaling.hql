set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_dummy_tbls;

use ${QESDUMMYBLS};

drop table if exists ${QESDUMMYBLS}.chatlp_cutoff_table;
create table chatlp_cutoff_table(
metric_name             string
,typology               string
,relationship_to_churn  string
,lower_bound            smallint
,upper_bound            smallint
,cutoff_one             decimal(20,4)
,cutoff_two             decimal(20,4)
,cutoff_three           decimal(20,4)
,cutoff_four            decimal(20,4)
,cutoff_five            decimal(20,4)
,insert_dt              date
) stored as orc tblproperties("orc.compress" = "SNAPPY");
-- threshold: 
-- cutoff 1: 
-- cutoff 2: 
-- cutoff 4: 
-- cutoff 5: 


insert overwrite table ${QESDUMMYBLS}.chatlp_cutoff_table values 
-- ('nature_score', 'all', 'positive', Null, Null, -2.278, -1.357, -1.05, 0.127, 0.135, '2019-10-08')
-- ('nature_score', 'all', 'negative', Null, Null, -1.077, -1.030, 0.05, 0.363, 0.366, '2020-07-23')
-- ('nature_score', 'all', 'negative', Null, Null, -0.56, -0.545, -0.11, 0.282, 0.359, '2020-10-14')
-- ('nature_score', 'all', 'negative', Null, Null, -5.6, -5.45, -0.75, 3.33, 3.59, '2020-10-14')
-- ('nature_score', 'all', 'negative', Null, Null, -6.06, -5.81, 1.65, 3.86, 4.32, '2020-10-27')
 ('nature_score', 'all', 'negative', Null, Null, -6.06, -5.1, 1.7, 3.25, 4.32, '2020-10-27')
; 
  
drop table if exists ${QESDUMMYBLS}.chatlp_nature_score_datamart;
create table chatlp_nature_score_datamart stored as orc tblproperties("orc.compress" = "SNAPPY") as
select a.mtn 
, a.month
, a.number_of_events
, a.nature_score
, case 
 when a.nature_score <= b.cutoff_one then 1
 when a.nature_score > b.cutoff_one and a.nature_score <= b.cutoff_two then 1 + (a.nature_score - b.cutoff_one) / (b.cutoff_two - b.cutoff_one)
 when a.nature_score > b.cutoff_two and a.nature_score <= b.cutoff_three then 2 + (a.nature_score - b.cutoff_two) / (b.cutoff_three - b.cutoff_two)
 when a.nature_score > b.cutoff_three and a.nature_score <= b.cutoff_four then 3 + (a.nature_score - b.cutoff_three) / (b.cutoff_four - b.cutoff_three)
 when a.nature_score > b.cutoff_four and a.nature_score <= b.cutoff_five then 4 + (a.nature_score - b.cutoff_four) / (b.cutoff_five - b.cutoff_four)
 when a.nature_score > b.cutoff_five then 5
 end as q_nature_score
from ${QESDUMMYBLS}.chatlp_nature_scores as a
 cross join (
 select  * 
 from ${QESDUMMYBLS}.chatlp_cutoff_table 
 where metric_name = 'nature_score'
) as b
;
