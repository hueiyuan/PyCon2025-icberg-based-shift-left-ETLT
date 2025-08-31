from pyspark.sql import SparkSession

def drop_tables():
    spark = SparkSession.builder \
        .appName("DropTables") \
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
    
    # Drop existing tables
    tables = [
        "nessie.iot_monitoring.raw_telemetry",
        "nessie.iot_monitoring.validated_telemetry",
        "nessie.iot_monitoring.anomaly_alerts"
    ]
    
    for table_name in tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table_name}")
            print(f"Dropped table '{table_name}'")
        except Exception as e:
            print(f"Error dropping table '{table_name}': {e}")
    
    spark.stop()

if __name__ == "__main__":
    drop_tables()