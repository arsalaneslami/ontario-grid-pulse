-- DBU consumption over the last 7 days
SELECT 
  sku_name,
  usage_date,
  ROUND(SUM(usage_quantity), 2) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY sku_name, usage_date
ORDER BY usage_date DESC, dbus DESC;
