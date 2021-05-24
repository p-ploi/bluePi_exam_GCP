CREATE OR REPLACE TABLE `{{ params.PROJECT_ID }}.{{ params.DWH_DATASET }}.USER_USER_LOG` AS
SELECT  
 TTIMESTAMP(created_at)       as created_at
, TTIMESTAMP(updated_at)       as updated_at
, id
, user_id
, action
, CASE WHEN status=0
    THEN False
    ELSE True
  END                                               as success
FROM
  `{{ params.PROJECT_ID }}.{{ params.STAGING_DATASET }}.user_user_log`