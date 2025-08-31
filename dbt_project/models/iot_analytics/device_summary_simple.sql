{{
  config(
    materialized='table'
  )
}}

-- Simple device summary for IoT monitoring
SELECT 
    device_id,
    device_type,
    COUNT(*) as total_readings,
    MIN(timestamp) as first_seen,
    MAX(timestamp) as last_seen,
    COUNT(DISTINCT DATE(from_unixtime(timestamp / 1000))) as active_days
FROM {{ source('iot_monitoring', 'validated_telemetry') }}
GROUP BY device_id, device_type