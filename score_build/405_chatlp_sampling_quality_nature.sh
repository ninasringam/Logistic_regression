#!/bin/bash
echo "This is a shell script for getting counts of topic from call_transcripts tables"

for var in 0 1 2 3 4 5 6 7 8 9
do
hive -hiveconf num=$var -e 'set num;set hive.cli.print.header=true;  
select q_nature_score as q_nature_score
, min_quality_score as min_quality_score
, avg_quality_score as avg_quality_score
, coalesce(churn_vol_m1_flag,0) as churn_vol_m1_flag
from qes_prd_tst_tbls.chatlp_customer_overall_v2
where month between 202005 and 202009
and q_nature_score is not null
and min_quality_score is not null
and avg_quality_score is not null
and churn_vol_m1_flag = 0
distribute by RAND(${hiveconf:num}) 
sort by RAND(${hiveconf:num})
limit 9000;' | sed 's/\t/,/g' |tail -n+2 > /homes/s_zw_o_vz_do_pw/qes/chatlp/level_2_sample_$var.csv

hive -hiveconf num=$var -e 'set num; select q_nature_score as q_nature_score
 , min_quality_score as min_quality_score
 , avg_quality_score as avg_quality_score
 , coalesce(churn_vol_m1_flag,0) as churn_vol_m1_flag
 from qes_prd_tst_tbls.chatlp_customer_overall_v2
 where month between 202005 and 202009
 and q_nature_score is not null
 and min_quality_score is not null
 and avg_quality_score is not null
 and churn_vol_m1_flag = 1
 distribute by RAND(${hiveconf:num}) 
 sort by RAND(${hiveconf:num})
 limit 9000;' | sed 's/\t/,/g' | tail -n +2 >> /homes/s_zw_o_vz_do_pw/qes/chatlp/level_2_sample_$var.csv

done


