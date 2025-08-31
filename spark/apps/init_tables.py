from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, MapType

def create_tables():
    spark = SparkSession.builder \
        .appName("InitializeTables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.type", "nessie") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://demo/warehouse") \
        .config("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS nessie.iot_monitoring")
    print("Database 'iot_monitoring' created or already exists")
    
    # Define schemas
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
    
    # Create schemas for different tables
    # Raw telemetry uses base schema
    raw_df = spark.createDataFrame([], telemetry_schema)
    
    # Validated and anomaly tables need additional quality columns
    quality_schema = telemetry_schema.add("quality_status", StringType(), True) \
        .add("anomaly_score", DoubleType(), True) \
        .add("validation_method", StringType(), True)
    
    quality_df = spark.createDataFrame([], quality_schema)
    
    # Create tables with appropriate schemas
    tables = [
        ("nessie.iot_monitoring.raw_telemetry", raw_df, "Raw telemetry data"),
        ("nessie.iot_monitoring.validated_telemetry", quality_df, "Validated telemetry data"),
        ("nessie.iot_monitoring.anomaly_alerts", quality_df, "Anomaly alerts")
    ]
    
    for table_name, schema_df, description in tables:
        try:
            # Check if table exists
            spark.sql(f"DESCRIBE TABLE {table_name}")
            print(f"Table '{table_name}' already exists")
        except:
            # Create table if it doesn't exist
            schema_df.writeTo(table_name) \
                .using("iceberg") \
                .create()
            print(f"Table '{table_name}' created - {description}")
    
    # Verify tables
    print("\nVerifying tables:")
    result = spark.sql("SHOW TABLES IN nessie.iot_monitoring")
    result.show()
    
    spark.stop()

if __name__ == "__main__":
    create_tables()