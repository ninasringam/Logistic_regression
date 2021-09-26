use qes_prd_dummy_tbls;
create table chatlp_model_weights (metric_name string, weight decimal(7,6)) stored as orc tblproperties("orc.compress" = "SNAPPY");
insert overwrite table chatlp_model_weights values
('q_agent_score',0.031166569064964674),
('q_app_bnc_3d_flag',0.018989847722664144),
('q_arr_resptime_avg',0.008210678418102328),
('q_arr_resptime_sum',0.00917241207898285),
('q_chatlp_bnc_3d_flipped_flag',0.011563132902289178),
('q_closed_by_agent_flag',0.04823847781860322),
('q_closed_by_timeout_flipped_flag',0.06810683704483755),
('q_ivr_bnc_3d_flipped_flag',0.06431006549988869),
('q_retail_bnc_3d_flag',0.059373944255599505),
('q_return_to_queue_flipped_flag',0.05710185020389843),
('q_sales_conversion_flag',0.4533800586424599),
('q_total_msg',0.02810255493438905),
('q_voice_bnc_3d_flipped_flag',0.05597550419274739),
('q_web_bnc_3d_flipped_flag',0.08630806722057316),
('min_quality_score',0.22266516315382173),
('q_nature_score',0.7773348368461783);

insert into table chatlp_model_weights values
('avg_quality_score',0.20994543847626496),
('q_nature_score',0.7900545615237351)

insert overwrite table chatlp_model_weights values
('q_agent_score',0.027416668088480187),
('q_app_bnc_3d_flag',0.02861521489398922),
('q_arr_resptime_avg',0.016136440811807574),
('q_arr_resptime_sum',0.008404942862371853),
('q_bnc_3d_flipped_flag',0.04516862500665649),
('q_chatbot_bnc_3d_flag',0.019714659222304077),
('q_closed_by_agent_flag',0.026019391139698),
('q_closed_by_customer_flipped_flag',0.0364167254287562),
('q_closed_by_timeout_flipped_flag',0.0849639712308599),
('q_ivr_bnc_3d_flipped_flag',0.050680591975817035),
('q_retail_bnc_3d_flag',0.06305973569955724),
('q_return_to_queue_flipped_flag',0.04899189059735212),
('q_sales_conversion_flag',0.4046741176828339),
('q_total_msg',0.02420444983004751),
('q_voice_bnc_3d_flipped_flag',0.04897520239637451),
('q_web_bnc_3d_flipped_flag',0.0665573731330942);

insert into table chatlp_model_weights values
('min_quality_score',0.2158557488880779),
('q_nature_score',0.7841442511119221)


insert overwrite table chatlp_model_weights values
('q_agent_score',0.02996338073173069),
('q_app_bnc_3d_flag',0.023083197958996854),
('q_arr_resptime_avg',0.007747562314728325),
('q_closed_by_agent_flag',0.029704765854761967),
('q_closed_by_customer_flipped_flag',0.03865655608882956),
('q_closed_by_timeout_flipped_flag',0.08877482128447686),
('q_ivr_bnc_3d_flipped_flag',0.05926522675727644),
('q_retail_bnc_3d_flag',0.06683758783077358),
('q_return_to_queue_flipped_flag',0.051454069957986584),
('q_sales_conversion_flag',0.44053015483017094),
('q_total_msg',0.03258340185462658),
('q_voice_bnc_3d_flipped_flag',0.05398724707028514),
('q_web_bnc_3d_flipped_flag',0.07741202746535653),
('min_quality_score',0.25992535620404295),
('q_nature_score',0.7400746437959571)


