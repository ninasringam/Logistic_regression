hive --hiveconf hive.tez.container.size=10240 yarn.scheduler.minimum-allocation-mb=1g -f /homes/s_zw_o_vz_do_pw/qes/chatlp/99_app_datamart_event_original_pre.hql 2>/homes/s_zw_o_vz_do_pw/qes/chatlp/logs/99_app_datamart_event_original_pre.log
hive --hiveconf hive.tez.container.size=10240 yarn.scheduler.minimum-allocation-mb=1g -f /homes/s_zw_o_vz_do_pw/qes/chatlp/100_app_datamart_event_original.hql 2>/homes/s_zw_o_vz_do_pw/qes/chatlp/logs/100_app_datamart_event_original.log
hive --hiveconf hive.tez.container.size=10240 yarn.scheduler.minimum-allocation-mb=1g -f /homes/s_zw_o_vz_do_pw/qes/chatlp/99_web_datamart_event_original_pre.hql 2>/homes/s_zw_o_vz_do_pw/qes/chatlp/logs/99_web_datamart_event_original_pre.log
hive --hiveconf hive.tez.container.size=10240 yarn.scheduler.minimum-allocation-mb=1g -f /homes/s_zw_o_vz_do_pw/qes/chatlp/100_web_datamart_event_original.hql 2>/homes/s_zw_o_vz_do_pw/qes/chatlp/logs/100_web_datamart_event_original.log
hive --hiveconf hive.tez.container.size=10240 yarn.scheduler.minimum-allocation-mb=1g -f /homes/s_zw_o_vz_do_pw/qes/chatlp/100_callcenter_datamart_event_original.hql 2>/homes/s_zw_o_vz_do_pw/qes/chatlp/logs/100_callcenter_datamart_event_original.log
hive --hiveconf hive.tez.container.size=10240 yarn.scheduler.minimum-allocation-mb=1g -f /homes/s_zw_o_vz_do_pw/qes/chatlp/100_chatbot_datamart_event_original.hql 2>/homes/s_zw_o_vz_do_pw/qes/chatlp/logs/100_chatbot_datamart_event_original.log
hive --hiveconf hive.tez.container.size=10240 yarn.scheduler.minimum-allocation-mb=1g -f /homes/s_zw_o_vz_do_pw/qes/chatlp/100_ivr_datamart_event_original.hql 2>/homes/s_zw_o_vz_do_pw/qes/chatlp/logs/100_ivr_datamart_event_original.log
hive --hiveconf hive.tez.container.size=10240 yarn.scheduler.minimum-allocation-mb=1g -f /homes/s_zw_o_vz_do_pw/qes/chatlp/100_retail_datamart_event_original.hql 2>/homes/s_zw_o_vz_do_pw/qes/chatlp/logs/100_retail_datamart_event_original.log
echo "Completed 100 Datamart Build"