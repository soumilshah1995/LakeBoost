"""
# Author: Soumil Nitin Shah
# Email: shahsoumil519@gmail.com

# Configurations
key                           value
--additional-python-modules  faker==11.3.0
--conf                        spark.serializer=org.apache.spark.serializer.KryoSerializer
                             --conf spark.sql.hive.convertMetastoreParquet=false
                             --conf spark.sql.hive.convertMetastoreParquet=false
                             --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog
                             --conf spark.sql.legacy.pathOptionBehavior.enabled=true
                             --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension
--datalake-formats           hudi


# Sample Payload
{
    "JOB_NAME": job_name,

    "SOURCE_S3_PATH": f"s3://{BUCKET_NAME}/uber/raw/table_name=users/",

    "ENABLE_CLEANER": "False",
    "ENABLE_HIVE_SYNC": "True",
    "ENABLE_PARTITION": "True",
    "GLUE_DATABASE": "uber",
    "GLUE_TABLE_NAME": "users",

    "HUDI_PRECOMB_KEY": "created_at",
    "HUDI_RECORD_KEY": "user_id",
    "HUDI_TABLE_TYPE": "COPY_ON_WRITE",
    "INDEX_TYPE" : "BLOOM"
    "PARTITION_FIELDS": "year,month",
    "SOURCE_FILE_TYPE": "parquet",

    "USE_SQL_TRANSFORMER": "True",
    "SQL_TRANSFORMER_QUERY": "SELECT * ,"
                             " extract(year from created_at) as year,"
                             " extract(month from created_at) as month,"
                             " extract(day from created_at) as day,"
                             " uuid() as user_dim_key,"
                             " true as is_current"
                             " FROM temp;",

    "TARGET_S3_PATH": f"s3://{BUCKET_NAME}/uber/silver/table_name=users/",
}

"""

# Import necessary modules
try:
    import os, uuid, sys, boto3, time, sys
    from pyspark.sql.functions import lit, udf
    import sys
    from pyspark.context import SparkContext
    from pyspark.sql.session import SparkSession
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
except Exception as e:
    print("Modules are missing : {} ".format(e))

# Get command-line arguments
args = getResolvedOptions(
    sys.argv, [
        'JOB_NAME',
        'SOURCE_S3_PATH',
        "SOURCE_FILE_TYPE",
        'GLUE_DATABASE',
        'GLUE_TABLE_NAME',
        "HUDI_TABLE_TYPE",
        'HUDI_PRECOMB_KEY',
        'HUDI_RECORD_KEY',
        "ENABLE_CLEANER",
        'ENABLE_HIVE_SYNC',
        "ENABLE_PARTITION",
        'INDEX_TYPE',
        "PARTITON_FIELDS",
        "USE_SQL_TRANSFORMER",
        'SQL_TRANSFORMER_QUERY',  # comma was missing here
        'TARGET_S3_PATH',

    ],
)
# Create a Spark session
spark = (SparkSession.builder.config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
         .config('spark.sql.hive.convertMetastoreParquet', 'false') \
         .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.hudi.catalog.HoodieCatalog') \
         .config('spark.sql.extensions', 'org.apache.spark.sql.hudi.HoodieSparkSessionExtension') \
         .config('spark.sql.legacy.pathOptionBehavior.enabled', 'true').getOrCreate())

# Create a Spark context and Glue context
sc = spark.sparkContext
glueContext = GlueContext(sc)
job = Job(glueContext)
logger = glueContext.get_logger()
job.init(args["JOB_NAME"], args)


def upsert_hudi_table(glue_database, table_name, record_id, precomb_key, table_type, spark_df,
                      enable_partition, enable_cleaner, enable_hive_sync, use_sql_transformer, sql_transformer_query,
                      target_path, index_type, method='upsert'):
    """
    Upserts a dataframe into a Hudi table.

    Args:
        glue_database (str): The name of the glue database.
        table_name (str): The name of the Hudi table.
        record_id (str): The name of the field in the dataframe that will be used as the record key.
        precomb_key (str): The name of the field in the dataframe that will be used for pre-combine.
        table_type (str): The Hudi table type (e.g., COPY_ON_WRITE, MERGE_ON_READ).
        spark_df (pyspark.sql.DataFrame): The dataframe to upsert.
        enable_partition (bool): Whether or not to enable partitioning.
        enable_cleaner (bool): Whether or not to enable data cleaning.
        enable_hive_sync (bool): Whether or not to enable syncing with Hive.
        use_sql_transformer (bool): Whether or not to use SQL to transform the dataframe before upserting.
        sql_transformer_query (str): The SQL query to use for data transformation.
        target_path (str): The path to the target Hudi table.
        method (str): The Hudi write method to use (default is 'upsert').
        index_type : BLOOM or GLOBAL_BLOOM
    Returns:
        None
    """
    # These are the basic settings for the Hoodie table
    hudi_final_settings = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": method,
        "hoodie.datasource.write.recordkey.field": record_id,
        "hoodie.datasource.write.precombine.field": precomb_key,
    }

    # These settings enable syncing with Hive
    hudi_hive_sync_settings = {
        "hoodie.parquet.compression.codec": "gzip",
        "hoodie.datasource.hive_sync.enable": "true",
        "hoodie.datasource.hive_sync.database": glue_database,
        "hoodie.datasource.hive_sync.table": table_name,
        "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
        "hoodie.datasource.hive_sync.use_jdbc": "false",
        "hoodie.datasource.hive_sync.mode": "hms",
    }

    # These settings enable automatic cleaning of old data
    hudi_cleaner_options = {
        "hoodie.clean.automatic": "true",
        "hoodie.clean.async": "true",
        "hoodie.cleaner.policy": 'KEEP_LATEST_FILE_VERSIONS',
        "hoodie.cleaner.fileversions.retained": "3",
        "hoodie-conf hoodie.cleaner.parallelism": '200',
        'hoodie.cleaner.commits.retained': 5
    }

    # These settings enable partitioning of the data
    partition_settings = {
        "hoodie.datasource.write.partitionpath.field": args['PARTITON_FIELDS'],
        "hoodie.datasource.hive_sync.partition_fields": args['PARTITON_FIELDS'],
        "hoodie.datasource.write.hive_style_partitioning": "true",
    }

    # Define a dictionary with the index settings for Hudi
    hudi_index_settings = {
        "hoodie.index.type": index_type,  # Specify the index type for Hudi
    }

    # Add the Hudi index settings to the final settings dictionary
    for key, value in hudi_index_settings.items():
        hudi_final_settings[key] = value  # Add the key-value pair to the final settings dictionary

    # If partitioning is enabled, add the partition settings to the final settings
    if enable_partition == "True" or enable_partition == "true" or enable_partition == True:
        for key, value in partition_settings.items(): hudi_final_settings[key] = value

    # If data cleaning is enabled, add the cleaner options to the final settings
    if enable_cleaner == "True" or enable_cleaner == "true" or enable_cleaner == True:
        for key, value in hudi_cleaner_options.items(): hudi_final_settings[key] = value

    # If Hive syncing is enabled, add the Hive sync settings to the final settings
    if enable_hive_sync == "True" or enable_hive_sync == "true" or enable_hive_sync == True:
        for key, value in hudi_hive_sync_settings.items(): hudi_final_settings[key] = value

    # If there is data to write, apply any SQL transformations and write to the target path
    if spark_df.count() > 0:
        if use_sql_transformer == "True" or use_sql_transformer == "true" or use_sql_transformer == True:
            spark_df.createOrReplaceTempView("temp")
            spark_df = spark.sql(sql_transformer_query)

        spark_df.write.format("hudi"). \
            options(**hudi_final_settings). \
            mode("append"). \
            save(target_path)


def read_data_s3(path, format, table_name):
    """
    Reads data from an S3 bucket using AWS Glue and returns a Spark DataFrame.

    Args:
        path (str): S3 bucket path where data is stored
        format (str): file format of the data (e.g., parquet)
        table_name (str): name of glue table
    Returns:
        spark_df (pyspark.sql.DataFrame): Spark DataFrame containing the data
    """

    if format == "parquet" or format == "json":
        transformation_ctx = f"S3bucket_{table_name}"

        # create a dynamic frame from the S3 bucket using AWS Glue
        glue_df = glueContext.create_dynamic_frame.from_options(
            format_options={},
            connection_type="s3",
            format=format,
            connection_options={
                "paths": [path],
                "recurse": True,
            },
            transformation_ctx=transformation_ctx,
        )

        # convert dynamic frame to Spark DataFrame
        spark_df = glue_df.toDF()

        # print the first few rows of the DataFrame
        print(spark_df.show())

        return spark_df


def run():
    # Read data from S3
    spark_df = read_data_s3(
        path=args.get('SOURCE_S3_PATH', ''),
        format=args.get('SOURCE_FILE_TYPE', ''),
        table_name=args.get('GLUE_TABLE_NAME', '')
    )

    # Upsert data to Hudi table
    upsert_hudi_table(
        glue_database=args.get('GLUE_DATABASE', ''),
        table_name=args.get('GLUE_TABLE_NAME', ''),
        record_id=args.get('HUDI_RECORD_KEY', ''),
        precomb_key=args.get('HUDI_PRECOMB_KEY', ''),
        table_type=args.get('HUDI_TABLE_TYPE', 'COPY_ON_WRITE'),
        method='upsert',
        index_type=args.get('INDEX_TYPE', 'BLOOM'),
        enable_partition=args.get('ENABLE_PARTITION', 'False'),
        enable_cleaner=args.get('ENABLE_CLEANER', 'False'),
        enable_hive_sync=args.get('ENABLE_HIVE_SYNC', 'True'),
        use_sql_transformer=args.get('USE_SQL_TRANSFORMER', 'False'),
        sql_transformer_query=args.get('SQL_TRANSFORMER_QUERY', 'default'),
        target_path=args.get('TARGET_S3_PATH', ''),
        spark_df=spark_df,
    )

    # Commit the job
    job.commit()


run()

# Call the run function
run()
