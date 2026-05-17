-- ABAC functions for row-level and column-level access control.
-- Stored in the ops schema and applied to Silver tables.

-- Row filter: admins see all, others see only data from last 2 years.
CREATE OR REPLACE FUNCTION ogp_dev.ops.filter_recent_only(event_ts_utc TIMESTAMP)
RETURN 
  is_account_group_member('admins')
  OR event_ts_utc >= current_timestamp() - INTERVAL '2 YEARS';

-- Column mask: admins see real station names, others see 'MASKED-STATION'.
CREATE OR REPLACE FUNCTION ogp_dev.ops.mask_station_name(name STRING)
RETURN 
  CASE 
    WHEN is_account_group_member('admins') THEN name
    ELSE 'MASKED-STATION'
  END;

-- Verify both functions exist
SHOW USER FUNCTIONS IN ogp_dev.ops;
