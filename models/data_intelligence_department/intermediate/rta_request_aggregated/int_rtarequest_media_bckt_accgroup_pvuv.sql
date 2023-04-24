{{ config(materialized='view') }}

with req_media_bckt_accgroup_pvuv_agg as (
   select * from {{ ref('stg_glaucus__rta_req_media_bckt_accgroup_pvuv_agg') }}
),
rtarequest_media_bckt_accgroup_pvuv as (
    select date_format(req.dt,'yyyyMMdd')as p_day
    , req.media
    , coalesce(req.bucket,'无')bucket
    , coalesce(req.config_id,'无')config_id
    , coalesce(cfg.strategys_name,'无')strategys_name
    , request_pv
    , release_pv
    , request_uv
    , release_uv
    from req_media_bckt_accgroup_pvuv_agg req left join {{ ref('stg_glaucus__rta_bckt_accgroup_config') }} cfg
        on req.config_id = cfg.config_id and req.bucket=cfg.bucket and cfg.input_date=date_sub(req.dt,1)
    where req.dt >= date_format(date_sub(date('{{ var("pday") }}') ,7),'yyyyMMdd')
        and   req.dt < date_format(date_add(date('{{ var("pday") }}') ,1),'yyyyMMdd')
)

select * from rtarequest_media_bckt_accgroup_pvuv



