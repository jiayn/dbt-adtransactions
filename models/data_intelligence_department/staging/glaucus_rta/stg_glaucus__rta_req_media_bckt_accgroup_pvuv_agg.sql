{{ config(materialized='view') }}

with source as (

    select * from {{ source('data_ai_glaucus_preagg', 'dm_eco_bm_rta_req_new_config_bckt') }} 

),
renamed as ( 
    select
          dt
        , case when media='腾讯'  then '广点通' else media end as media
        , REVERSE(LEFT( REVERSE(strategy_name),LOCATE('_' , REVERSE(strategy_name) )-1))config_id
        , bucket
        , request_pv
        , release_pv
        , request_uv
        , release_uv
    from source
)

select * from renamed

