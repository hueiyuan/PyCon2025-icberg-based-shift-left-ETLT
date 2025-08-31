{{
  config(
    materialized='table'
  )
}}

-- Real-time energy consumption dashboard by building
WITH current_hour_energy AS (
    SELECT
        metadata.location.building as building,
        metadata.location.floor as floor,
        metadata.location.zone as zone,
        device_id,
        AVG(CAST(readings['power'] AS DOUBLE)) as current_power_w,
        AVG(CAST(readings['voltage'] AS DOUBLE)) as current_voltage,
        AVG(CAST(readings['current'] AS DOUBLE)) as current_amperage,
        AVG(CAST(readings['power_factor'] AS DOUBLE)) as power_factor
    FROM {{ source('iot_monitoring', 'validated_telemetry') }}
    WHERE device_type = 'energy_meter'
        AND timestamp >= to_unixtime(current_timestamp - interval '1' hour) * 1000
    GROUP BY 
        metadata.location.building,
        metadata.location.floor,
        metadata.location.zone,
        device_id
),

daily_energy_stats AS (
    SELECT
        building,
        COUNT(DISTINCT device_id) as meter_count,
        SUM(current_power_w) as total_current_power_w,
        SUM(current_power_w) / 1000 as total_current_power_kw,
        AVG(current_voltage) as avg_voltage,
        AVG(power_factor) as avg_power_factor,
        -- Estimate daily consumption based on current rate
        (SUM(current_power_w) * 24) / 1000 as estimated_daily_kwh,
        -- Estimate monthly cost (assuming $0.12 per kWh)
        (SUM(current_power_w) * 24 * 30) / 1000 * 0.12 as estimated_monthly_cost_usd
    FROM current_hour_energy
    GROUP BY building
),

floor_breakdown AS (
    SELECT
        building,
        floor,
        COUNT(DISTINCT device_id) as meter_count,
        SUM(current_power_w) as floor_power_w,
        SUM(current_power_w) / 1000 as floor_power_kw
    FROM current_hour_energy
    GROUP BY building, floor
),

anomaly_summary AS (
    SELECT
        metadata.location.building as building,
        COUNT(*) as anomaly_count_today
    FROM {{ source('iot_monitoring', 'anomaly_alerts') }}
    WHERE device_type = 'energy_meter'
        AND timestamp >= to_unixtime(date_trunc('day', current_timestamp)) * 1000
    GROUP BY metadata.location.building
)

SELECT
    des.building,
    des.meter_count,
    des.total_current_power_kw,
    des.avg_voltage,
    des.avg_power_factor,
    des.estimated_daily_kwh,
    des.estimated_monthly_cost_usd,
    COALESCE(ans.anomaly_count_today, 0) as energy_anomalies_today,
    -- Energy efficiency score (0-100)
    CASE
        WHEN des.avg_power_factor >= 0.95 AND ans.anomaly_count_today = 0 THEN 100
        WHEN des.avg_power_factor >= 0.90 AND ans.anomaly_count_today <= 2 THEN 80
        WHEN des.avg_power_factor >= 0.85 AND ans.anomaly_count_today <= 5 THEN 60
        ELSE 40
    END as efficiency_score,
    -- Recommendations
    CASE
        WHEN des.avg_power_factor < 0.85 THEN 'Install power factor correction equipment'
        WHEN ans.anomaly_count_today > 5 THEN 'Investigate voltage irregularities'
        WHEN des.total_current_power_kw > 500 THEN 'Consider load balancing'
        ELSE 'Operating normally'
    END as recommendation,
    CURRENT_TIMESTAMP as dashboard_updated_at
FROM daily_energy_stats des
LEFT JOIN anomaly_summary ans ON des.building = ans.building
ORDER BY des.total_current_power_kw DESC