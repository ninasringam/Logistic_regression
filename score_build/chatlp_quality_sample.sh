hive -e 'set hive.cli.print.header=true; select q_duration as q_duration
, q_total_msg as q_total_msg
, q_closed_by_customer_flag as q_closed_by_customer_flag
, q_return_to_queue_flag as q_return_to_queue_flag
, q_web_bnc_3d_flag as q_web_bnc_3d_flag
, q_voice_bnc_3d_flag as q_voice_bnc_3d_flag
, q_ivr_bnc_3d_flipped_flag as q_ivr_bnc_3d_flipped_flag
, q_bnc_3d_flipped_flag as q_bnc_3d_flipped_flag
, q_closed_by_timeout_flipped_flag as q_closed_by_timeout_flipped_flag
, q_agent_transfer_flipped_flag as q_agent_transfer_flipped_flag
, q_multiple_agent_flipped_flag as q_multiple_agent_flipped_flag
, q_retail_bnc_3d_flipped_flag as q_retail_bnc_3d_flipped_flag
, q_sales_conversion_flipped_flag as q_sales_conversion_flipped_flag
, q_detractor_flipped_flag as q_detractor_flipped_flag
, q_promoter_flag as q_promoter_flag
, q_agent_score as q_agent_score
, coalesce(churn_vol_m1_flag,0) as churn_vol_m1_flag
 from qes_prd_dummy_tbls.chatlp_customer_scored_v1
 where month between '202004' and '202006'
 and q_duration is not null
 and q_total_msg is not null
 and q_closed_by_customer_flag is not null
 and q_return_to_queue_flag is not null
 and q_web_bnc_3d_flag is not null
 and q_voice_bnc_3d_flag is not null
 and q_ivr_bnc_3d_flipped_flag is not null
 and q_bnc_3d_flipped_flag is not null
 and q_closed_by_timeout_flipped_flag is not null
 and q_agent_transfer_flipped_flag is not null
 and q_multiple_agent_flipped_flag is not null
 and q_retail_bnc_3d_flipped_flag is not null
 and q_sales_conversion_flipped_flag is not null
 and q_detractor_flipped_flag is not null
 and q_promoter_flag is not null
 and q_agent_score is not null
 and churn_vol_m1_flag = 0
 distribute by RAND(1) 
 sort by RAND(1)
 limit 5000;' | sed 's/\t/,/g' > /homes/s_zw_o_vz_do_pw/qes/chatlp/sample_1.csv
 
hive -e 'select q_duration as q_duration
, q_total_msg as q_total_msg
, q_closed_by_customer_flag as q_closed_by_customer_flag
, q_return_to_queue_flag as q_return_to_queue_flag
, q_web_bnc_3d_flag as q_web_bnc_3d_flag
, q_voice_bnc_3d_flag as q_voice_bnc_3d_flag
, q_ivr_bnc_3d_flipped_flag as q_ivr_bnc_3d_flipped_flag
, q_bnc_3d_flipped_flag as q_bnc_3d_flipped_flag
, q_closed_by_timeout_flipped_flag as q_closed_by_timeout_flipped_flag
, q_agent_transfer_flipped_flag as q_agent_transfer_flipped_flag
, q_multiple_agent_flipped_flag as q_multiple_agent_flipped_flag
, q_retail_bnc_3d_flipped_flag as q_retail_bnc_3d_flipped_flag
, q_sales_conversion_flipped_flag as q_sales_conversion_flipped_flag
, q_detractor_flipped_flag as q_detractor_flipped_flag
, q_promoter_flag as q_promoter_flag
, q_agent_score as q_agent_score
, coalesce(churn_vol_m1_flag,0) as churn_vol_m1_flag
 from qes_prd_dummy_tbls.chatlp_customer_scored_v1
 where month between '202004' and '202006'
 and q_duration is not null
 and q_total_msg is not null
 and q_closed_by_customer_flag is not null
 and q_return_to_queue_flag is not null
 and q_web_bnc_3d_flag is not null
 and q_voice_bnc_3d_flag is not null
 and q_ivr_bnc_3d_flipped_flag is not null
 and q_bnc_3d_flipped_flag is not null
 and q_closed_by_timeout_flipped_flag is not null
 and q_agent_transfer_flipped_flag is not null
 and q_multiple_agent_flipped_flag is not null
 and q_retail_bnc_3d_flipped_flag is not null
 and q_sales_conversion_flipped_flag is not null
 and q_detractor_flipped_flag is not null
 and q_promoter_flag is not null
 and q_agent_score is not null
 and churn_vol_m1_flag = 1
 distribute by RAND(1) 
 sort by RAND(0)
 limit 5000;' | sed 's/\t/,/g' >> /homes/s_zw_o_vz_do_pw/qes/chatlp/sample_0.csv