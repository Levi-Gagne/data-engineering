from pyspark.sql import SparkSession
from contextlib import contextmanager

@contextmanager
def initialize_spark_session(script_name: str):
    """
    Initializes a Spark session with predefined settings optimized for performance and usability.
    This function is a context manager to ensure that the Spark session is properly closed after use.
    
    Parameters:
        script_name (str): The name of the application, useful for identifying the Spark application on the cluster.
    
    Usage:
        with initialize_spark_session('MySparkApp') as spark:
            # Your Spark code here
    """

    # Configuration of the Spark session
    spark = (
        SparkSession.builder
        .appName(script_name)  # Set the name of the application
        .config("spark.sql.caseSensitive", "false")  # Case-insensitive SQL queries
        .config("spark.sql.parquet.binaryAsString", "true")  # Handle binary data in Parquet files as strings
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")  # AWS credentials for S3 access
        .config("spark.dynamicAllocation.enabled", "true")  # Enable dynamic allocation of executors
        .config("spark.dynamicAllocation.initialExecutors", 2)  # Initial number of executors
        .config("spark.dynamicAllocation.minExecutors", 1)  # Minimum number of executors
        .config("spark.dynamicAllocation.maxExecutors", 10)  # Maximum number of executors
        .config("spark.sql.adaptive.enabled", "true")  # Enable adaptive query execution
        .config("spark.executor.memory", "20g")  # Memory per executor
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # Use Kryo serializer for better performance
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC")  # Use G1GC for garbage collection
        .config("spark.speculation", "true")  # Speculative execution of slow tasks
        .config("spark.sql.session.timeZone", "America/Detroit")  # Set the time zone for the session
        .config("spark.sql.shuffle.partitions", "200")  # Default number of shuffle partitions
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600")  # Threshold for broadcast joins (100MB)
        .config("spark.rdd.compress", "true")  # Compress RDDs
        .config("spark.shuffle.compress", "true")  # Compress data shuffled between executors
        .config("spark.shuffle.spill.compress", "true")  # Compress data spilled during shuffling
        .config("spark.sql.parquet.enableVectorizedReader", "true")  # Enable vectorized Parquet reader
        .enableHiveSupport()  # Enable support for Hive features
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')  # Set log level to WARN to reduce verbosity

    successfully_closed = False
    try:
        yield spark  # Yield the spark session to be used in the 'with' block
    finally:
        if spark is not None:
            try:
                spark.stop()  # Stop the Spark session
                successfully_closed = True
            except Exception as e:
                print(f"Error closing Spark session: {e}")

        if not successfully_closed:
            print("Spark session wasn't closed successfully. Additional cleanup or retry logic here.")
