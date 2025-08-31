# PyCon2025-icberg-based-shift-left-ETLT

This demo showcases a comprehensive shift-left data architecture for IoT device monitoring using Kafka, Spark Streaming, Iceberg, and dbt.

## Architecture Components

### 1. **Apache Kafka (KRaft Mode)**

- Event streaming platform without Zookeeper dependency
- Handles real-time data ingestion
- Topic: `user-events`

### 2. **Schema Registry**

- Centralized schema management
- Ensures data contract compliance
- Validates schema evolution

### 3. **Apache Spark Structured Streaming**

- Real-time IoT telemetry processing
- Anomaly detection for predictive maintenance
- Multi-stage quality validation

### 4. **Apache Iceberg + Nessie**

- ACID-compliant data lake table format
- Git-like version control for data
- Branch isolation for development

### 5. **dbt (Data Build Tool)**

- Business logic transformations
- Device health scoring
- Energy consumption analytics
- Hourly aggregations for trend analysis

### 6. **Quality Gates**

- **Schema Validation**: Ensures telemetry conforms to expected structure
- **Anomaly Detection**: Identifies out-of-range readings and device malfunctions
- **Dead Letter Queue**: Captures failed records for analysis

## Getting Started

### 1. Start the Infrastructure

```bash
docker-compose up -d
```

Wait for all services to be healthy:

```bash
docker-compose ps
```

### 2. Initialize Spark Dependencies

Enter the Spark container and run the initialization script:

```bash
docker exec -it spark bash
chmod +x /apps/init_kafka_deps.sh
/apps/init_kafka_deps.sh
```

### 3. Install Python Dependencies for Producer

In the Spark container:

```bash
pip install -r /apps/requirements.txt
```

### 4. Create Kafka Topic

```bash
docker exec -it kafka kafka-topics --create \
  --topic iot-telemetry \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 5. Run the IoT Producer

Generate IoT telemetry data from simulated devices:

```bash
docker exec -it spark python /apps/kafka_producer.py \
  --bootstrap-servers kafka:29092 \
  --rate 10 \
  --scenario mixed
```

Producer scenarios:

- `valid`: Normal sensor readings
- `invalid_schema`: Malformed telemetry data
- `anomaly`: Out-of-range readings indicating device issues
- `edge_case`: Offline devices, sensor failures
- `mixed`: Realistic mix of all scenarios (70% valid, 30% issues)

### 6. Run the Streaming Consumer

In another terminal:

```bash
docker exec -it spark spark-submit \
  /apps/streaming_main.py
```

### 7. Query the IoT Data

Using Trino to analyze IoT telemetry:

```bash
docker exec -it trino-coordinator trino
```

See `/spark/apps/iot_dashboard_queries.sql` for comprehensive queries including:

- Real-time device status
- Anomaly detection summary
- Building temperature heatmaps
- Energy consumption trends
- Device health checks
- Predictive maintenance alerts

### 8. Run dbt Transformations

Transform raw telemetry into business insights:

```bash
docker exec -it dbt bash
cd /usr/app/dbt
dbt run --models iot_analytics
```

This creates:

- `device_health_summary`: Device health scores and maintenance recommendations
- `hourly_sensor_aggregates`: Time-series aggregations for trend analysis
- `building_energy_dashboard`: Real-time energy consumption metrics

### 10. Access dbt Documentation

dbt documentation is automatically generated and served at:

```
http://localhost:8082
```

The documentation includes:

- Data lineage visualization
- Model descriptions and tests
- Column-level documentation
- Source data catalog

Alternatively, generate docs manually:

```bash
docker exec -it dbt bash
cd /usr/app/dbt
dbt docs generate
dbt docs serve --port 8080 --host 0.0.0.0
```

## IoT Use Case Benefits

### 1. **Predictive Maintenance**

- Early detection of device anomalies
- Vibration pattern analysis for mechanical issues
- Battery level monitoring
- Automated maintenance recommendations

### 2. **Energy Optimization**

- Real-time power consumption monitoring
- Building-level energy analytics
- Power factor optimization alerts
- Cost estimation and budgeting

### 3. **Data Quality for IoT**

- Validates sensor readings at ingestion
- Detects stuck or malfunctioning sensors
- Handles offline devices gracefully
- Preserves raw data for troubleshooting

### 4. **Scalable Architecture**

- Handles millions of telemetry messages
- Partitioned by device for parallel processing
- Time-series optimized storage
- Branch-based development for safe testing

## Testing Different Scenarios

### Simulate Device Anomalies

```bash
# Generate anomalous readings (temperature spikes, vibration issues)
docker exec -it spark python /apps/kafka_producer.py \
  --bootstrap-servers kafka:29092 \
  --scenario anomaly \
  --rate 5 \
  --duration 60
```

### Simulate Device Failures

```bash
# Generate edge cases (offline devices, sensor failures)
docker exec -it spark python /apps/kafka_producer.py \
  --bootstrap-servers kafka:29092 \
  --scenario edge_case \
  --rate 2 \
  --duration 30
```

### Development Branch Testing

```bash
# Create a new branch in Nessie
curl -X POST http://localhost:19120/api/v1/trees/tree \
  -H "Content-Type: application/json" \
  -d '{"name": "feature-branch", "ref": {"type": "BRANCH", "name": "main"}}'

# Run consumer on feature branch
docker exec -it spark bash -c "NESSIE_BRANCH=feature-branch spark-submit /apps/kafka_streaming_consumer.py"
```

## Monitoring

### Check Kafka Topic

```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic iot-telemetry \
  --from-beginning \
  --max-messages 10
```

### Monitor Streaming Jobs

Check Spark streaming progress:

```bash
docker exec -it spark bash -c "ls -la /tmp/spark-checkpoints/main/"
```

### MinIO Console

Access at http://localhost:9091

- Username: minioadmin
- Password: minioadmin

### Schema Registry

Check registered schemas:

```bash
curl http://localhost:8081/subjects
```

## Cleanup

Stop all services:

```bash
docker-compose down -v
```

