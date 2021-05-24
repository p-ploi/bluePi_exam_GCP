CREATE OR REPLACE TABLE `{{ params.PROJECT_ID }}.{{ params.DWH_DATASET }}.USER_USERS` AS
SELECT 
  TIMESTAMP_MICROS(cast(created_at AS INT64))      as created_at
, TIMESTAMP_MICROS(cast(updated_at AS INT64))     as updated_at
, id
, first_name
, last_name
, current_timestamp()                             as dl_load_dt
FROM
  `{{ params.PROJECT_ID }}.{{ params.STAGING_DATASET }}.user_users`