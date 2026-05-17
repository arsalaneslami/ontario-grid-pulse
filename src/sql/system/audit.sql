-- Recent UC operations (last 24 hours)
SELECT 
  event_time,
  user_identity.email     AS user,
  service_name,
  action_name,
  request_params.full_name_arg AS object,
  response.status_code
FROM system.access.audit
WHERE event_time >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY event_time DESC
LIMIT 20;
