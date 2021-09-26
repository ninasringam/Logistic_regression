use qes_prd_tbls;
create table chatlp_typology_lookup stored as orc tblproperties("orc.compress" = "SNAPPY") as
select call_reason_high_lvl as low_level, call_reason_high_lvl_typology as delta_assigned_high_level_reason 
from qes_prd_tbls.voice_call_reason_high_lvl_typology_lookup;

drop table if exists chatlp_typology_lookup;
create table chatlp_typology_lookup
 (
 low_level                           string
 ,Delta_assigned_high_level_reason   string
 ) stored as orc tblproperties('orc.compress'='SNAPPY')
; 

insert into table chatlp_typology_lookup values 
 ('Activate', 'Activate'),
 ('Disconnect', 'Disconnect'),
 ('Troubleshooting', 'Support'),
 ('Rewards', 'Rewards'),
 ('Bill', 'Billing'),
 ('Plan', 'Plan Change'),
 ('Upgrade', 'Sales and Upgrade'),
 ('Payment', 'Payment'),
 ('Learn', 'Learn'),
 ('Equipment', 'Device'),
 ('Account Management', 'Account Management'),
 ('International', 'Account Management'),
 ('Usage', 'Use');
 