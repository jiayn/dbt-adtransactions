{{ config(materialized='view') }}

with req_media_bckt_accgroup_agg_costs as (
    select * from {{ source('data_ai_glaucus_preagg', 'dm_eco_bm_rta_bucket_cfgid_cost') }} 
),
rtarequest_media_bckt_accgroup_costs as (
    select date_format(req.dt,'yyyyMMdd')as p_day
    , qd as media
    , coalesce(req.bucket,'无')bucket
    , coalesce(req.config_id,'无')config_id
    , coalesce(cfg.strategys_name,'无')strategys_name
    , costs
    from req_media_bckt_accgroup_agg_costs req left join {{ ref('stg_glaucus__rta_bckt_accgroup_config') }} cfg
        on      req.config_id = cfg.config_id 
            and req.bucket=cfg.bucket 
            and from_unixtime(unix_timestamp(cfg.input_pday,'yyyyMMdd'),'yyyy-MM-dd')=date_sub(req.dt,1)
    where req.dt >= date_format(date_sub(date('{{ var("pday") }}') ,7),'yyyyMMdd')
        and   req.dt < date_format(date_add(date('{{ var("pday") }}') ,1),'yyyyMMdd')
)

select * from rtarequest_media_bckt_accgroup_costs