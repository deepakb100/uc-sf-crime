import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# Done Create a schema for incoming resources
schema = StructType([
    StructField('crime_id', StringType(), True), 
    StructField('original_crime_type_name', StringType(), True), 
    StructField('report_date', StringType(), True), 
    StructField('call_date', StringType(), True), 
    StructField('offense_date', StringType(), True), 
    StructField('call_time', StringType(), True), 
    StructField('call_date_time', StringType(), True), 
    StructField('disposition', StringType(), True), 
    StructField('address', StringType(), True), 
    StructField('city', StringType(), True), 
    StructField('state', StringType(), True), 
    StructField('agency_id', StringType(), True), 
    StructField('address_type', StringType(), True), 
    StructField('common_location', StringType(), True)
])

def run_spark_job(spark):

    # Done Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "police-service-calls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 200) \
        .option("maxRatePerPartition", 10) \
        .option("stopGracefullyOnShutdown", "true") \
        .option("group.id", "abc-123") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Done extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    service_table.createOrReplaceTempView("service_calls")


    #Done select original_crime_type_name and disposition
    # count the number of original crime type
    # Done Q1. Submit a screen shot of a batch ingestion of the aggregation
    # Done write output stream
    query = spark.sql('''
        select original_crime_type_name, count(1) from service_calls group by original_crime_type_name
    ''').writeStream \
        .outputMode('complete') \
        .format('console') \
        .option("truncate", "false") \
        .start()

    agg_df = spark.sql('''
        select original_crime_type_name, disposition from service_calls
    ''')


   

    # Done get the right radio code json path
    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.json(radio_code_json_filepath)


    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # Done rename disposition_code column to disposition
    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")

    radio_code_df.printSchema()

    # Done join on disposition column
    join_query = agg_df.join(radio_code_df, "disposition")

    join_query.writeStream.outputMode("append").format("console").start().awaitTermination()
    
    # Done attach a ProgressReporter
    query.awaitTermination()

if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # Done Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
