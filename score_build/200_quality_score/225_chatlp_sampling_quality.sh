#!/bin/bash
echo "This is a shell script for getting counts of topic from call_transcripts tables"

for var in 0 1 2 3 4 5 6 7 8 9
do
   hive -hiveconf num=$var -e 'set num;set hive.cli.print.header=true; 
select q_arr_resptime_sum  as q_arr_resptime_sum
, q_arr_resptime_avg  as q_arr_resptime_avg
, q_total_msg as q_total_msg
, q_arr_q as q_arr_q
, q_chat_connection_score as q_chat_connection_score
, q_ivr_bnc_3d_flipped_flag as q_ivr_bnc_3d_flipped_flag
, q_return_to_queue_flipped_flag as q_return_to_queue_flipped_flag
, q_web_bnc_3d_flipped_flag as q_web_bnc_3d_flipped_flag
, q_voice_bnc_3d_flipped_flag as q_voice_bnc_3d_flipped_flag
, q_app_bnc_3d_flag as q_app_bnc_3d_flag
, q_bnc_3d_flipped_flag as q_bnc_3d_flipped_flag
, q_chatbot_bnc_3d_flag as q_chatbot_bnc_3d_flag
, q_agent_transfer_flag as q_agent_transfer_flag
, q_multiple_agent_flag as q_multiple_agent_flag
, q_retail_bnc_3d_flag as q_retail_bnc_3d_flag 
, q_sales_conversion_flag as q_sales_conversion_flag
, q_closed_by_agent_flag as q_closed_by_agent_flag
, q_closed_by_customer_flipped_flag as q_closed_by_customer_flipped_flag
, q_closed_by_timeout_flipped_flag as q_closed_by_timeout_flipped_flag
, q_chatlp_bnc_3d_flipped_flag as q_chatlp_bnc_3d_flipped_flag
, q_detractor_flipped_flag as q_detractor_flipped_flag
, q_promoter_flag as q_promoter_flag
, q_agent_score as q_agent_score
, coalesce(churn_vol_m1_flag,0) as churn_vol_m1_flag
 from qes_prd_tst_tbls.chatlp_customer_scored_v1
 where month between '202005' and '202009'
 and q_arr_resptime_sum is not null
 and q_arr_resptime_avg is not null
 and q_total_msg is not null
 and q_arr_q is not null
 and q_chat_connection_score is not null
 and q_ivr_bnc_3d_flipped_flag is not null
 and q_return_to_queue_flipped_flag is not null
 and q_web_bnc_3d_flipped_flag is not null
 and q_voice_bnc_3d_flipped_flag is not null
 and q_app_bnc_3d_flag is not null
 and q_bnc_3d_flipped_flag is not null
 and q_chatbot_bnc_3d_flag is not null
 and q_agent_transfer_flag is not null
 and q_closed_by_customer_flipped_flag is not null
 and q_multiple_agent_flag is not null
 and q_retail_bnc_3d_flag is not null
 and q_sales_conversion_flag is not null
 and q_closed_by_agent_flag is not null
 and q_closed_by_timeout_flipped_flag is not null
 and q_chatlp_bnc_3d_flipped_flag is not null
 and q_detractor_flipped_flag is not null
 and q_promoter_flag is not null
 and q_agent_score is not null
 and churn_vol_m1_flag = 0
 distribute by RAND(${hiveconf:num}) 
 sort by RAND(${hiveconf:num})
 limit 9000;' | sed 's/\t/,/g' |tail -n +2 >/homes/s_zw_o_vz_do_pw/qes/chatlp/sample_$var.csv

hive -hiveconf num=$var -e 'set num; 
select q_arr_resptime_sum as q_arr_resptime_sum
, q_arr_resptime_avg  as q_arr_resptime_avg
, q_total_msg as q_total_msg
, q_arr_q as q_arr_q
, q_chat_connection_score as q_chat_connection_score
, q_ivr_bnc_3d_flipped_flag as q_ivr_bnc_3d_flipped_flag
, q_return_to_queue_flipped_flag as q_return_to_queue_flipped_flag
, q_web_bnc_3d_flipped_flag as q_web_bnc_3d_flipped_flag
, q_voice_bnc_3d_flipped_flag as q_voice_bnc_3d_flipped_flag
, q_app_bnc_3d_flag as q_app_bnc_3d_flag
, q_bnc_3d_flipped_flag as q_bnc_3d_flipped_flag
, q_chatbot_bnc_3d_flag as q_chatbot_bnc_3d_flag
, q_agent_transfer_flag as q_agent_transfer_flag
, q_multiple_agent_flag as q_multiple_agent_flag
, q_retail_bnc_3d_flag as q_retail_bnc_3d_flag 
, q_sales_conversion_flag as q_sales_conversion_flag
, q_closed_by_agent_flag as q_closed_by_agent_flag
, q_closed_by_customer_flipped_flag as q_closed_by_customer_flipped_flag
, q_closed_by_timeout_flipped_flag as q_closed_by_timeout_flipped_flag
, q_chatlp_bnc_3d_flipped_flag as q_chatlp_bnc_3d_flipped_flag
, q_detractor_flipped_flag as q_detractor_flipped_flag
, q_promoter_flag as q_promoter_flag
, q_agent_score as q_agent_score
, coalesce(churn_vol_m1_flag,0) as churn_vol_m1_flag
 from qes_prd_tst_tbls.chatlp_customer_scored_v1
 where month between '202005' and '202009'
 and q_arr_resptime_sum is not null
 and q_arr_resptime_avg is not null
 and q_total_msg is not null
 and q_arr_q is not null
 and q_chat_connection_score is not null
 and q_ivr_bnc_3d_flipped_flag is not null
 and q_return_to_queue_flipped_flag is not null
 and q_web_bnc_3d_flipped_flag is not null
 and q_voice_bnc_3d_flipped_flag is not null
 and q_app_bnc_3d_flag is not null
 and q_bnc_3d_flipped_flag is not null
 and q_chatbot_bnc_3d_flag is not null
 and q_agent_transfer_flag is not null
 and q_multiple_agent_flag is not null
 and q_retail_bnc_3d_flag is not null
 and q_sales_conversion_flag is not null
 and q_closed_by_agent_flag is not null
 and q_closed_by_customer_flipped_flag is not null
 and q_closed_by_timeout_flipped_flag is not null
 and q_chatlp_bnc_3d_flipped_flag is not null
 and q_detractor_flipped_flag is not null
 and q_promoter_flag is not null
 and q_agent_score is not null
 and churn_vol_m1_flag = 1
 distribute by RAND(${hiveconf:num})
 sort by RAND(${hiveconf:num})
 limit 9000;' | sed 's/\t/,/g'|tail -n +2 >>/homes/s_zw_o_vz_do_pw/qes/chatlp/sample_$var.csv

done



