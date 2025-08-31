-- IoT Monitoring Dashboard Queries
-- Run these queries in Trino after the streaming pipeline has processed some data

-- 1. Real-time Device Status Overview
-- Shows current status of all devices with their latest readings
SELECT 
    device_id,
    device_type,
    metadata.location.building as building,
    metadata.location.floor as floor,
    metadata.location.zone as zone,
    quality_status,
    FROM_UNIXTIME(timestamp / 1000) as last_reading_time,
    readings
FROM nessie.iot_monitoring.validated_telemetry
WHERE timestamp >= to_unixtime(CURRENT_TIMESTAMP - INTERVAL '1' HOUR) * 1000
ORDER BY timestamp DESC
LIMIT 20;

-- 2. Anomaly Detection Summary
-- Shows devices with the most anomalies in the last 24 hours
SELECT 
    device_id,
    device_type,
    COUNT(*) as anomaly_count,
    AVG(anomaly_score) as avg_anomaly_score,
    MAX(anomaly_score) as max_anomaly_score,
    ARRAY_AGG(DISTINCT metadata.anomaly_type) as anomaly_types
FROM nessie.iot_monitoring.anomaly_alerts
WHERE timestamp >= to_unixtime(CURRENT_TIMESTAMP - INTERVAL '24' HOUR) * 1000
GROUP BY device_id, device_type
ORDER BY anomaly_count DESC
LIMIT 10;

-- 3. Building Temperature Heatmap Data
-- Average temperature by building and floor for visualization
SELECT 
    metadata.location.building as building,
    metadata.location.floor as floor,
    AVG(CAST(readings['temperature'] AS DOUBLE)) as avg_temperature,
    MIN(CAST(readings['temperature'] AS DOUBLE)) as min_temperature,
    MAX(CAST(readings['temperature'] AS DOUBLE)) as max_temperature,
    COUNT(DISTINCT device_id) as sensor_count
FROM nessie.iot_monitoring.validated_telemetry
WHERE device_type IN ('temperature_sensor', 'pressure_sensor', 'vibration_sensor')
    AND timestamp >= CAST(CURRENT_TIMESTAMP - INTERVAL '1' HOUR AS BIGINT) * 1000
    AND readings['temperature'] IS NOT NULL
GROUP BY metadata.location.building, metadata.location.floor
ORDER BY building, floor;

-- 4. Energy Consumption Trends
-- Hourly energy consumption for the past 24 hours
WITH hourly_energy AS (
    SELECT 
        DATE_TRUNC('hour', FROM_UNIXTIME(timestamp / 1000)) as hour,
        metadata.location.building as building,
        SUM(CAST(readings['power'] AS DOUBLE)) / 1000 as total_power_kw
    FROM nessie.iot_monitoring.validated_telemetry
    WHERE device_type = 'energy_meter'
        AND timestamp >= CAST(CURRENT_TIMESTAMP - INTERVAL '24' HOUR AS BIGINT) * 1000
    GROUP BY DATE_TRUNC('hour', FROM_UNIXTIME(timestamp / 1000)), metadata.location.building
)
SELECT 
    hour,
    building,
    total_power_kw,
    SUM(total_power_kw) OVER (PARTITION BY building ORDER BY hour) as cumulative_kwh
FROM hourly_energy
ORDER BY building, hour;

-- 5. Device Health Check
-- Devices that haven't reported in the last hour (potential failures)
WITH latest_readings AS (
    SELECT 
        device_id,
        MAX(timestamp) as last_seen_timestamp
    FROM nessie.iot_monitoring.raw_telemetry
    GROUP BY device_id
)
SELECT 
    lr.device_id,
    vt.device_type,
    vt.metadata.location.building as building,
    vt.metadata.location.floor as floor,
    FROM_UNIXTIME(lr.last_seen_timestamp / 1000) as last_seen,
    to_unixtime(CURRENT_TIMESTAMP) * 1000 - lr.last_seen_timestamp as silence_duration_ms
FROM latest_readings lr
JOIN (
    SELECT DISTINCT device_id, device_type, metadata
    FROM nessie.iot_monitoring.validated_telemetry
) vt ON lr.device_id = vt.device_id
WHERE lr.last_seen_timestamp < to_unixtime(CURRENT_TIMESTAMP - INTERVAL '1' HOUR) * 1000
ORDER BY silence_duration_ms DESC;

-- 6. Vibration Analysis for Predictive Maintenance
-- Devices with increasing vibration patterns (potential mechanical issues)
WITH vibration_trends AS (
    SELECT 
        device_id,
        DATE_TRUNC('hour', FROM_UNIXTIME(timestamp / 1000)) as hour,
        AVG(SQRT(
            POWER(CAST(readings['vibration_x'] AS DOUBLE), 2) +
            POWER(CAST(readings['vibration_y'] AS DOUBLE), 2) +
            POWER(CAST(readings['vibration_z'] AS DOUBLE), 2)
        )) as avg_vibration_magnitude
    FROM nessie.iot_monitoring.validated_telemetry
    WHERE device_type = 'vibration_sensor'
        AND timestamp >= CAST(CURRENT_TIMESTAMP - INTERVAL '24' HOUR AS BIGINT) * 1000
    GROUP BY device_id, DATE_TRUNC('hour', FROM_UNIXTIME(timestamp / 1000))
)
SELECT 
    device_id,
    MIN(avg_vibration_magnitude) as min_vibration,
    MAX(avg_vibration_magnitude) as max_vibration,
    MAX(avg_vibration_magnitude) - MIN(avg_vibration_magnitude) as vibration_increase,
    -- Calculate trend direction
    CASE 
        WHEN MAX(avg_vibration_magnitude) - MIN(avg_vibration_magnitude) > 0.5 THEN 'INCREASING'
        WHEN MAX(avg_vibration_magnitude) - MIN(avg_vibration_magnitude) < -0.5 THEN 'DECREASING'
        ELSE 'STABLE'
    END as trend
FROM vibration_trends
GROUP BY device_id
HAVING MAX(avg_vibration_magnitude) - MIN(avg_vibration_magnitude) > 0.3
ORDER BY vibration_increase DESC;

-- 7. Data Quality Analysis
-- Understanding data quality distribution
SELECT 
    quality_status,
    COUNT(*) as record_count,
    COUNT(DISTINCT device_id) as device_count,
    AVG(anomaly_score) as avg_anomaly_score,
    MIN(FROM_UNIXTIME(timestamp / 1000)) as first_occurrence,
    MAX(FROM_UNIXTIME(timestamp / 1000)) as last_occurrence
FROM nessie.iot_monitoring.validated_telemetry
WHERE timestamp >= to_unixtime(CURRENT_TIMESTAMP - INTERVAL '24' HOUR) * 1000
GROUP BY quality_status
ORDER BY record_count DESC;

-- 8. Battery Status Alert
-- Devices with low battery that need immediate attention
SELECT 
    device_id,
    device_type,
    metadata.location.building as building,
    metadata.location.floor as floor,
    metadata.location.zone as zone,
    metadata.battery_level as current_battery_level,
    FROM_UNIXTIME(timestamp / 1000) as reading_time
FROM nessie.iot_monitoring.validated_telemetry
WHERE metadata.battery_level IS NOT NULL 
    AND metadata.battery_level < 30
    AND timestamp >= CAST(CURRENT_TIMESTAMP - INTERVAL '1' HOUR AS BIGINT) * 1000
ORDER BY metadata.battery_level ASC
LIMIT 20;