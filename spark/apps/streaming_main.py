import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, current_timestamp, to_timestamp, when, abs as spark_abs
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, MapType

def main():
    """
    IoT PySpark Structured Streaming job demonstrating comprehensive "Shift-Left" approach:
    1. Schema validation for IoT telemetry
    2. Anomaly detection and quality gates
    3. Dead letter queue for failed records
    4. Isolated development using Nessie branches
    """
    
    # Get or create SparkSession (will use the one from spark-submit)
    dev_branch = os.getenv("NESSIE_BRANCH", "main")
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")
    
    spark = SparkSession.builder \
        .appName("IoT-Shift-Left-Demo") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.type", "nessie") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.ref", dev_branch) \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://demo/warehouse") \
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session with Kafka support created successfully.")
    
    # Configuration from environment variables
    kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    
    schema_name = "nessie.iot_monitoring"
    raw_table = f"{schema_name}.raw_telemetry"
    validated_table = f"{schema_name}.validated_telemetry"
    anomaly_table = f"{schema_name}.anomaly_alerts"
    dlq_table = f"{schema_name}.dead_letter_queue"
    
    checkpoint_base = "/tmp/spark-checkpoints"
    
    # # Configure Nessie catalog using Iceberg's Spark Catalog
    # spark.conf.set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    # spark.conf.set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    # spark.conf.set("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1")
    # spark.conf.set("spark.sql.catalog.nessie.ref", dev_branch)
    # spark.conf.set("spark.sql.catalog.nessie.warehouse", "s3a://demo/warehouse")
    
    # # Configure S3 access for MinIO
    # spark.conf.set("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    # spark.conf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
    # spark.conf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    # spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
    # spark.conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    
    # Create schema if it doesn't exist
    print(f"\nUsing branch '{dev_branch}' for all operations...")
    print(f"Creating schema {schema_name} if not exists...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}").show()
    
    # Define IoT telemetry schema
    location_schema = StructType([
        StructField("building", StringType(), True),
        StructField("floor", IntegerType(), True),
        StructField("zone", StringType(), True)
    ])
    
    metadata_schema = StructType([
        StructField("location", location_schema, True),
        StructField("firmware_version", StringType(), True),
        StructField("signal_strength", IntegerType(), True),
        StructField("battery_level", IntegerType(), True),
        StructField("anomaly_detected", StringType(), True),
        StructField("anomaly_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("error", StringType(), True)
    ])
    
    telemetry_schema = StructType([
        StructField("event_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("timestamp", LongType(), False),
        StructField("readings", MapType(StringType(), DoubleType()), False),
        StructField("metadata", metadata_schema, True)
    ])
    
    # Note: Parsing and quality gates are now applied within the foreachBatch function
    
    # Define normal ranges for anomaly detection
    normal_ranges = {
        "temperature_sensor": {"temperature": (18.0, 25.0), "humidity": (30.0, 60.0)},
        "pressure_sensor": {"pressure": (1000.0, 1020.0), "temperature": (15.0, 30.0)},
        "vibration_sensor": {
            "vibration_x": (-2.0, 2.0), "vibration_y": (-2.0, 2.0), 
            "vibration_z": (-2.0, 2.0), "temperature": (20.0, 40.0)
        },
        "energy_meter": {
            "voltage": (220.0, 240.0), "current": (0.0, 30.0),
            "power": (0.0, 7000.0), "power_factor": (0.85, 0.99)
        }
    }
    
    # Quality Gate 2: Anomaly Detection
    # Check for out-of-range values, stuck sensors, and timestamp issues
    anomaly_conditions = (
        # Temperature anomalies
        ((col("device_type") == "temperature_sensor") & 
         ((col("readings.temperature") < 18.0) | (col("readings.temperature") > 25.0) |
          (col("readings.humidity") < 30.0) | (col("readings.humidity") > 60.0))) |
        # Pressure anomalies
        ((col("device_type") == "pressure_sensor") & 
         ((col("readings.pressure") < 1000.0) | (col("readings.pressure") > 1020.0))) |
        # Vibration anomalies
        ((col("device_type") == "vibration_sensor") & 
         ((spark_abs(col("readings.vibration_x")) > 2.0) |
          (spark_abs(col("readings.vibration_y")) > 2.0) |
          (spark_abs(col("readings.vibration_z")) > 2.0))) |
        # Energy anomalies
        ((col("device_type") == "energy_meter") & 
         ((col("readings.voltage") < 220.0) | (col("readings.voltage") > 240.0) |
          (col("readings.power") > 7000.0))) |
        # Timestamp anomalies (future or very old)
        (col("timestamp") > expr("unix_timestamp() * 1000 + 3600000")) |  # 1 hour future
        (col("timestamp") < expr("unix_timestamp() * 1000 - 86400000 * 7"))  # 7 days old
    )
    
    # Note: The actual filtering and processing is now done in the foreachBatch function
    # This allows for more flexible batch-level processing and better error handling
    
    # Define foreachBatch function to process all data streams
    def process_batch(batch_df, batch_id):
        """
        Process each micro-batch with all quality gates and write to multiple tables
        """
        print(f"Processing batch {batch_id}...")
        
        # Parse Kafka messages
        parsed_df = batch_df.select(
            col("key").cast("string").alias("kafka_key"),
            from_json(col("value").cast("string"), telemetry_schema).alias("data"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp")
        ).select(
            "kafka_key",
            "data.*",
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            current_timestamp().alias("processing_time")
        )
        
        # Quality Gate 1: Schema Validation
        valid_schema_df = parsed_df.filter(col("event_id").isNotNull())
        invalid_schema_df = parsed_df.filter(col("event_id").isNull())
        
        # Quality Gate 2: Enhanced Spark-based Data Quality Checks
        # Since Great Expectations 1.0+ removed SparkDFDataset, we implement validation using pure Spark
        
        # Quality Gate 3: Anomaly Detection using Spark rules
        validated_df = valid_schema_df.filter(
            ~anomaly_conditions
        ) \
            .withColumn("quality_status", expr("'PASSED'")) \
            .withColumn("anomaly_score", expr("0.0")) \
            .withColumn("validation_method", expr("'SPARK_RULES'"))
        
        anomaly_df = valid_schema_df.filter(
            anomaly_conditions
        ) \
            .withColumn("quality_status", expr("'ANOMALY_DETECTED'")) \
            .withColumn("anomaly_score", 
                        when(col("metadata.anomaly_detected") == "true", 1.0)
                        .otherwise(0.8)) \
            .withColumn("validation_method", expr("'SPARK_RULES'"))
        
        # Write all tables in the same batch
        # 1. Write raw data for audit
        if not parsed_df.isEmpty():
            print(f"  Writing raw records to '{raw_table}'...")
            # Select only the columns that match the table schema
            raw_df = parsed_df.select(
                "event_id", "device_id", "device_type", 
                "timestamp", "readings", "metadata"
            )
            raw_df.write \
                .format("iceberg") \
                .mode("append") \
                .save(raw_table)
        
        # 2. Write validated telemetry
        if not validated_df.isEmpty():
            print(f"  Writing validated records to '{validated_table}'...")
            # Select only the columns that match the table schema plus quality columns
            validated_write_df = validated_df.select(
                "event_id", "device_id", "device_type", 
                "timestamp", "readings", "metadata",
                "quality_status", "anomaly_score", "validation_method"
            )
            validated_write_df.write \
                .format("iceberg") \
                .mode("append") \
                .save(validated_table)
        
        # 3. Write anomaly alerts
        if not anomaly_df.isEmpty():
            print(f"  Writing anomaly records to '{anomaly_table}'...")
            # Select only the columns that match the table schema plus quality columns
            anomaly_write_df = anomaly_df.select(
                "event_id", "device_id", "device_type", 
                "timestamp", "readings", "metadata",
                "quality_status", "anomaly_score", "validation_method"
            )
            anomaly_write_df.write \
                .format("iceberg") \
                .mode("append") \
                .save(anomaly_table)
        
        # 4. Write dead letter records (if needed)
        # invalid_count = invalid_schema_df.count()
        # if invalid_count > 0:
        #     print(f"  Writing {invalid_count} invalid records to '{dlq_table}'...")
        #     dlq_df = invalid_schema_df.withColumn("error_type", expr("'SCHEMA_VALIDATION_FAILED'")) \
        #         .withColumn("error_timestamp", current_timestamp())
        #     dlq_df.write \
        #         .format("iceberg") \
        #         .mode("append") \
        #         .save(dlq_table)
        
        print(f"Batch {batch_id} processing completed.")
    
    # Single streaming query using foreachBatch
    print(f"Starting unified streaming job on branch '{dev_branch}'...")
    
    # Read from Kafka to Iceberg
    load_kafka_setting = {
        "subscribe": "iot-telemetry",
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "kafka.bootstrap.servers": kafka_bootstrap_servers
    }
    
    query = spark.readStream \
        .format("kafka") \
        .options(**load_kafka_setting) \
        .load() \
        .writeStream \
        .foreachBatch(process_batch) \
        .trigger(processingTime='10 seconds') \
        .option("checkpointLocation", f"{checkpoint_base}/{dev_branch}/unified") \
        .start()
    
    # Monitor the stream
    print("\nIoT Streaming job started. Monitoring progress...")
    print("Press Ctrl+C to stop.\n")
    
    try:
        # Wait for the stream
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping stream...")
        query.stop()
        print("Stream stopped.")
    
    print(f"\nStreaming job finished. Data written to branch '{dev_branch}'.")
    print(f"Validated telemetry: {validated_table}")
    print(f"Anomaly alerts: {anomaly_table}")
    print(f"Dead letter queue: {dlq_table}")
    print(f"Raw telemetry: {raw_table}")
    
    spark.stop()

if __name__ == "__main__":
    main()
