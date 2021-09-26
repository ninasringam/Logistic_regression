hive -e 'set hive.cli.print.header=true;  
select q_nature_score as q_nature_score
, min_quality_score as min_quality_score
, avg_quality_score as avg_quality_score
, coalesce(churn_vol_m1_flag,0) as churn_vol_m1_flag
from qes_prd_dummy_tbls.chatlp_customer_overall_v2
where month between 202004 and 202006
and q_nature_score is not null
and min_quality_score is not null
and avg_quality_score is not null
and churn_vol_m1_flag = 0
distribute by RAND(0) 
sort by RAND(0)
limit 10000;' | sed 's/\t/,/g' > /homes/s_zw_o_vz_do_pw/qes/chatlp/level_2_sample_0.csv

hive -e 'select q_nature_score as q_nature_score
 , min_quality_score as min_quality_score
 , avg_quality_score as avg_quality_score
 , coalesce(churn_vol_m1_flag,0) as churn_vol_m1_flag
 from qes_prd_dummy_tbls.chatlp_customer_overall_v2
 where month between 202004 and 202006
 and q_nature_score is not null
 and min_quality_score is not null
 and avg_quality_score is not null
 and churn_vol_m1_flag = 1
 distribute by RAND(0) 
 sort by RAND(0)
 limit 10000;' | sed 's/\t/,/g' >> /homes/s_zw_o_vz_do_pw/qes/chatlp/level_2_sample_0.csv