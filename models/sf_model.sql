{{
  config({    
    "materialized": "incremental",
    "database":  var('EDW_DATABASE'),
    "incremental_strategy": "merge",
    "merge_exclude_columns": ["SF_CREATE_DATE", "USER_INFORMATION_KEY", "SF_DEL_FLAG"],
    "unique_key": ["USER_ID"]
  })
}}

select
    6 as user_id,
    4 as SF_CREATE_DATE,
    5 as USER_INFORMATION_KEY,
    '{{ var('EDW_DATABASE') }}' as SF_DEL_FLAG
