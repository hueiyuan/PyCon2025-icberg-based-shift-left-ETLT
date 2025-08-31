{{
  config(
    materialized='table'
  )
}}

-- Latest metrics for each device
WITH latest_readings AS (
    SELECT 
        device_id,
        device_type,
        timestamp,
        readings,
        metadata,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY timestamp DESC) as rn
    FROM {{ source('iot_monitoring', 'validated_telemetry') }}
)

SELECT 
    device_id,
    device_type,
    metadata.location.building as building,
    metadata.location.floor as floor,
    metadata.location.zone as zone,
    timestamp as last_reading_timestamp,
    from_unixtime(timestamp / 1000) as last_reading_time,
    -- Extract specific readings based on device type
    CASE 
        WHEN device_type = 'temperature_sensor' THEN 
            'Temp: ' || CAST(readings['temperature'] AS VARCHAR) || '°C, Humidity: ' || CAST(readings['humidity'] AS VARCHAR) || '%'
        WHEN device_type = 'pressure_sensor' THEN 
            'Pressure: ' || CAST(readings['pressure'] AS VARCHAR) || ' hPa'
        WHEN device_type = 'energy_meter' THEN 
            'Power: ' || CAST(readings['power'] AS VARCHAR) || ' W, Voltage: ' || CAST(readings['voltage'] AS VARCHAR) || ' V'
        WHEN device_type = 'vibration_sensor' THEN 
            'Vibration X: ' || CAST(readings['vibration_x'] AS VARCHAR)
        ELSE 'N/A'
    END as latest_readings,
    metadata.battery_level,
    metadata.signal_strength
FROM latest_readings
WHERE rn = 1