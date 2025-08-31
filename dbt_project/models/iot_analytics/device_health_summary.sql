{{
  config(
    materialized='table',
    unique_key='device_id'
  )
}}

-- Device health summary aggregating telemetry data to identify devices needing maintenance
WITH latest_telemetry AS (
    SELECT 
        device_id,
        device_type,
        timestamp,
        readings,
        metadata,
        quality_status,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) as rn
    FROM {{ source('iot_monitoring', 'validated_telemetry') }}
    WHERE timestamp >= to_unixtime(current_timestamp - interval '24' hour) * 1000
),

device_metrics AS (
    SELECT
        device_id,
        device_type,
        COUNT(*) as total_readings_24h,
        AVG(CAST(metadata.signal_strength AS DOUBLE)) as avg_signal_strength,
        AVG(CAST(metadata.battery_level AS DOUBLE)) as avg_battery_level,
        MIN(CAST(metadata.battery_level AS INTEGER)) as min_battery_level,
        MAX(timestamp) as last_seen_timestamp
    FROM {{ source('iot_monitoring', 'validated_telemetry') }}
    WHERE timestamp >= to_unixtime(current_timestamp - interval '24' hour) * 1000
    GROUP BY device_id, device_type
),

anomaly_counts AS (
    SELECT
        device_id,
        COUNT(*) as anomaly_count_24h,
        MAX(anomaly_score) as max_anomaly_score
    FROM {{ source('iot_monitoring', 'anomaly_alerts') }}
    WHERE timestamp >= to_unixtime(current_timestamp - interval '24' hour) * 1000
    GROUP BY device_id
)

SELECT
    lt.device_id,
    lt.device_type,
    lt.metadata.location.building as building,
    lt.metadata.location.floor as floor,
    lt.metadata.location.zone as zone,
    lt.metadata.firmware_version as firmware_version,
    dm.total_readings_24h,
    dm.avg_signal_strength,
    dm.avg_battery_level,
    dm.min_battery_level,
    COALESCE(ac.anomaly_count_24h, 0) as anomaly_count_24h,
    COALESCE(ac.max_anomaly_score, 0.0) as max_anomaly_score,
    -- Health score calculation (0-100)
    CASE 
        WHEN dm.min_battery_level < 20 THEN 0
        WHEN ac.anomaly_count_24h > 10 THEN 20
        WHEN dm.avg_signal_strength < 60 THEN 40
        WHEN ac.anomaly_count_24h > 5 THEN 60
        WHEN dm.min_battery_level < 50 THEN 80
        ELSE 100
    END as health_score,
    -- Maintenance recommendation
    CASE
        WHEN dm.min_battery_level < 20 THEN 'URGENT: Replace battery'
        WHEN ac.anomaly_count_24h > 10 THEN 'URGENT: Device malfunction'
        WHEN dm.avg_signal_strength < 60 THEN 'Check connectivity'
        WHEN ac.anomaly_count_24h > 5 THEN 'Schedule inspection'
        WHEN dm.min_battery_level < 50 THEN 'Plan battery replacement'
        ELSE 'Healthy'
    END as maintenance_recommendation,
    from_unixtime(dm.last_seen_timestamp / 1000) as last_seen,
    CURRENT_TIMESTAMP as updated_at
FROM latest_telemetry lt
JOIN device_metrics dm ON lt.device_id = dm.device_id
LEFT JOIN anomaly_counts ac ON lt.device_id = ac.device_id
WHERE lt.rn = 1