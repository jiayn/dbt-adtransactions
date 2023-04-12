{{ config(materialized='view') }}

with front as (
select
p_day,        
case when length(imp_device_id)=32 then imp_device_id
           when length(imp_idfa_md5)=32 then imp_idfa_md5
           when length(imp_imei_md5)=32 then imp_imei_md5
           when length(imp_oaid_md5)=32 then imp_oaid_md5
           when length(imp_oaid) between 10 and 90 then md5(imp_oaid)
           when length(imp_android_id)=32 then imp_android_id
           else null end as imp_device_md5,
         case when length(clk_device_id)=32 then clk_device_id
           when length(clk_idfa_md5)=32 then clk_idfa_md5
           when length(clk_imei_md5)=32 then clk_imei_md5
           when length(clk_oaid_md5)=32 then clk_oaid_md5
           when length(clk_oaid) between 10 and 90 then md5(clk_oaid)
           when length(clk_android_id)=32 then clk_android_id
           else null end as clk_device_md5
from  dp_data_db.ad_trans_baidu_feb_03071 
)

select * from front
