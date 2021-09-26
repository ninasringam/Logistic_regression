set hive.execution.engine=tez;
set tez.queue.name=vci_qes;
set hivevar:QESPRDVW=qes_prd_scoring_allvm;
set hivevar:QESPRDTBLS=qes_prd_tbls;
set hivevar:QESSTGTBLS=qes_prdstg_tbls;
set hivevar:QESMLTBLS=qes_prd_ml_tbls;
set hivevar:QESDUMMYBLS=qes_prd_dummy_tbls;

use ${QESDUMMYBLS};

drop table if exists ${QESDUMMYBLS}.chatlp_customer_overall;
create table chatlp_customer_overall stored as orc tblproperties("orc.compress" = "SNAPPY") as
select  
coalesce(a.mtn,b.mtn) as mtn
, coalesce(a.month,b.month) as month
, a.event_cnt
, a.avg_quality_score
, a.min_quality_score
, b.nature_score
, b.q_nature_score
-- , (a.avg_quality_score - 1) * 25 as quality_score_final
, (a.min_quality_score - 1) * 25 as quality_score_final
, (b.q_nature_score - 1) * 25 as q_nature_score_final
, case
when a.min_quality_score is not null and b.q_nature_score is not null 
 then (a.min_quality_score * qty.weight) + (b.q_nature_score * ntr.weight)
when a.min_quality_score is null and b.q_nature_score is not null
 then b.q_nature_score
when a.min_quality_score is not null and b.q_nature_score is null
 then a.min_quality_score
end as q_chat_score
, case
when a.min_quality_score is not null and b.q_nature_score is not null 
 then (((a.min_quality_score * qty.weight) + (b.q_nature_score * ntr.weight)) - 1) * 25
when a.min_quality_score is null and b.q_nature_score is not null
 then (b.q_nature_score - 1) * 25
when a.min_quality_score is not null and b.q_nature_score is null
 then (a.min_quality_score - 1) * 25
end as q_chat_score_final
from ${QESDUMMYBLS}.chatlp_customer_scored as a
 full outer join ${QESDUMMYBLS}.chatlp_nature_score_datamart as b
 on  a.mtn = b.mtn
 and a.month = b.month
 cross join (select * from ${QESDUMMYBLS}.chatlp_model_weights where metric_name = 'min_quality_score') as qty 
 cross join (select * from ${QESDUMMYBLS}.chatlp_model_weights where metric_name = 'q_nature_score') as ntr
;

--------- avg ---
drop table if exists ${QESDUMMYBLS}.chatlp_customer_overall;
create table chatlp_customer_overall stored as orc tblproperties("orc.compress" = "SNAPPY") as
select  
coalesce(a.mtn,b.mtn) as mtn
, coalesce(a.month,b.month) as month
, a.event_cnt
, a.avg_quality_score
, a.min_quality_score
, b.nature_score
, b.q_nature_score
, (a.avg_quality_score - 1) * 25 as quality_score_final
-- , (a.min_quality_score - 1) * 25 as quality_score_final
, (b.q_nature_score - 1) * 25 as q_nature_score_final
, case
when a.avg_quality_score is not null and b.q_nature_score is not null 
 then (a.avg_quality_score * qty.weight) + (b.q_nature_score * ntr.weight)
when a.avg_quality_score is null and b.q_nature_score is not null
 then b.q_nature_score
when a.avg_quality_score is not null and b.q_nature_score is null
 then a.avg_quality_score
end as q_chat_score
, case
when a.avg_quality_score is not null and b.q_nature_score is not null 
 then (((a.avg_quality_score * qty.weight) + (b.q_nature_score * ntr.weight)) - 1) * 25
when a.avg_quality_score is null and b.q_nature_score is not null
 then (b.q_nature_score - 1) * 25
when a.avg_quality_score is not null and b.q_nature_score is null
 then (a.avg_quality_score - 1) * 25
end as q_chat_score_final
from ${QESDUMMYBLS}.chatlp_customer_scored as a
 full outer join ${QESDUMMYBLS}.chatlp_nature_score_datamart as b
 on  a.mtn = b.mtn
 and a.month = b.month
 cross join (select * from ${QESDUMMYBLS}.chatlp_model_weights where metric_name = 'avg_quality_score') as qty 
 cross join (select * from ${QESDUMMYBLS}.chatlp_model_weights where metric_name = 'q_nature_score') as ntr
;

======== working on below  =====================
-- Validation queries
select      floor(a.avg_quality_score)                      as quality_score
            , count(*)                                      as total_records
            , sum(b.churn_vol_m1_flag)                      as churners
            , 'Quality | Customer'             as granularity
            , '201810 to 201903'                            as timewindow
            , 'chat'                                         as platform
from        deltap_prd_qmtbls.chat_customer_overall   as a
            left join deltap_prd_qmtbls.customer_datamart   as b
            on  a.msisdn = b.msisdn
                and a.month = b.month
where       a.month between 201810 and 201903 and nullif(a.msisdn,'') is not null
group by    floor(a.avg_quality_score)
;
select      floor(a.q_nature_score)                         as quality_score
            , count(*)                                      as total_records
            , sum(b.churn_vol_m1_flag)                      as churners
            , 'Nature | Customer'              as granularity
            , '201810 to 201903'                            as timewindow
            , 'chat'                                         as platform
from        deltap_prd_qmtbls.chat_customer_overall   as a
            left join deltap_prd_qmtbls.customer_datamart   as b
            on  a.msisdn = b.msisdn
                and a.month = b.month
where       a.month between 201810 and 201903 and nullif(a.msisdn,'') is not null
group by    floor(a.q_nature_score)
;

select      floor(a.q_chat_score)                            as quality_score
            , count(*)                                      as total_records
            , sum(b.churn_vol_m1_flag)                      as churners
            , 'Overall | Customer'         as granularity
            , '201810 to 201903'                            as timewindow
            , 'chat'                                         as platform
from        deltap_prd_qmtbls.chat_customer_overall   as a
            left join deltap_prd_qmtbls.customer_datamart   as b
            on  a.msisdn = b.msisdn
                and a.month = b.month
where       a.month between 201810 and 201903 and nullif(a.msisdn,'') is not null
group by    floor(a.q_chat_score)
;

select      floor(a.quality_score)                          as quality_score
            , count(*)                                      as total_records
            , sum(b.churn_vol_m1_flag)                      as churners
            , 'Quality | Event'                             as granularity
            , '201810 to 201903'                            as timewindow
            , 'chat'                                         as platform
from        deltap_prd_qmtbls.chat_events_scored            as a
            left join deltap_prd_qmtbls.customer_datamart   as b
            on  a.msisdn = b.msisdn
                and a.month = b.month
where       a.month between 201810 and 201903 and nullif(a.msisdn,'') is not null
group by    floor(a.quality_score)
;

-- Extracting 'effective' weights
select avg(case when avg_quality_score is not null and q_nature_score is not null then qty.weight
 when avg_quality_score is not null and q_nature_score is null then 1
 else 0 end) as avg_qty_weight
 ,avg(case when avg_quality_score is not null and q_nature_score is not null then ntr.weight
 when avg_quality_score is null and q_nature_score is not null then 1
 else 0 end) as avg_ntr_weight
from chatlp_customer_overall  as a
 cross join (select weight from chatlp_model_weights where metric_name = 'avg_quality_score') as qty
 cross join (select weight from chatlp_model_weights where metric_name = 'q_nature_score') as ntr
 where avg_quality_score is not null or q_nature_Score is not null
;