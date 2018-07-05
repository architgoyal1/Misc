from pyspark                                import SparkConf, SparkContext
from pyspark.sql                            import SparkSession
from pyspark.sql                            import SQLContext
from pyspark.sql.functions                  import to_date, max, count
from snowplow_analytics_sdk.run_manifests   import *

import json
import sys
import argparse
import time
import datetime
import boto3
import snowplow_analytics_sdk.event_transformer

APP_NAME = 'Snowplow Example'
process_start_time = datetime.datetime.now() #time needs to be 24h (applies for every time in this script)

def transform_from_hybrid_to_json(line):
    transform = snowplow_analytics_sdk.event_transformer.transform(line)
    return json.dumps(transform)

def run_spark_job(spark, input_path, output_base_path):
    sc = spark.sparkContext
    transformed = sc.textFile(input_path).map(transform_from_hybrid_to_json) # possible performance improvement if you can use dataframe API
    # Import JSON to Spark Dataframe
    df = spark.read.json(transformed)
    #split dataframe by app_id (one for TEST and one for PROD)
    mask = df['app_id'] = 'TEST'
    df.persist()
    df_prod = df.select('*').where(col('app_id') == 'PROD')
    # df_prod = df[~mask]
    df_test = df.select('*').where(col('app_id') == 'TEST')
    # df_test = df[mask]

    #For filename
    #populate from current time: format (YYYY-mm-DD hh:MM:SS)
    s = str(datetime.datetime.now())
    #numbers below depend on format
    YYYYMMDD = s[0:3] + s[5:6] + s[8:9]
    HHMMSS = s[11:12] + s[14:15] + s[17:18]
    date_time = YYYYMMDD + "-" + HHMMSS

    #write dataframe out into S3 [how??]
    df_prod.write.format('json').save(output_base_path + '/' +'SNP_PROD_'+ YYYYMMDD +'-'+ HHMMSS )
    df_test.write.format('json').save(output_base_path + '/' +'SNP_TEST_'+ YYYYMMDD +'-'+ HHMMSS )

    #SPLIT FOR DF_TEST AND DF_PROD
    #create summary of information
    #count number of rows --> gives number of events
    #assume etl timestamp is 'data time'; and 'time_start/end' are pyspark run times
    # log_table_prod            = df_prod.agg( "SNP_PROD_"+date_time                  .alias("data_folder_name"),\
    #                                         count("*")                              .alias("number_of_events"),\
    #                                         max("etl_tstamp").astype('timestamp')   .alias("data_time_end")),\
    #                                         min("etl_tstamp").astype('timestamp')   .alias("data_time_start"),\
    #                                         process_start_time                      .alias("process_start_time"),\
    #                                         datetime.datetime.now()                 .alias("process_end_time"))

    # log_table_test            = df_test.agg( "SNP_TEST_"+date_time                  .alias("data_folder_name"),\
    #                                         count("*")                              .alias("number_of_events"),\
    #                                         max("etl_tstamp").astype('timestamp')   .alias("data_time_end")),\
    #                                         min("etl_tstamp").astype('timestamp')   .alias("data_time_start"),\
    #                                         process_start_time                      .alias("process_start_time"),\
    #                                         datetime.datetime.now()                 .alias("process_end_time"))
    # log_table_test.write.format('csv').options(delimiter='|', header = 'true').save('<<enter s3 bucket name for new data here>>' +a+"SNP_"+a+"TEST"+a+"_"+a+ YYYYMMDD +a+"-"+a+ HHMMSS +a+"_log.csv")
    # log_table_prod.write.format('csv').options(delimiter='|', header = 'true').save('<<enter s3 bucket name for new data here>>' +a+"SNP_"+a+"PROD"+a+"_"+a+ YYYYMMDD +a+"-"+a+ HHMMSS +a+"_log.csv")

    # Aweful way: return (df_prod.agg(count(), max(), min).collect(), df_test.agg(count(), max(), min).collect())
    # ((prod_count, prod_max_ts, prod_min_ts), (test_count, test_max_ts, test_min_ts)) = run_spark_job(...)
    # TODO: Use spark to get back the count and the max and the min to the driver using collect()
    # e.g. (not real code) (count, max_ts, min_ts) = df.agg(count(), max(), min).collect()
    # end time = datetime.datetime.now()
    # write to s3 from driver (in a new def)


#the next line of code is included in playwire script - not sure what it does?
###    log_table.write.jdbc(url, "logbook", mode="append", properties=properties)

def get_run_manifests( region='us-east-1', dynamo_db_table='snowplow-run-manifests'): 
    #TODO: region as command line argument
    #TODO: dynamo_db_table from command line arguments
    dynamodb_client = session.client('dynamodb', region_name=region)
    return RunManifests(dynamodb_client, dynamo_db_table)

def run_for_new_run_ids(spark, bucket, output_prefix='output' ):
    enriched_events_archive = 's3a://' + bucket + '/enriched/good/' #Probably archive instead fo good
    output_base_path = "s3a://" +  bucket + '/' + output_prefix + '/'
    
    # Retrieve run manifests
    session = boto3.Session()
    s3_client = session.client('s3')
    run_ids = list_runids(s3_client, enriched_events_archive)

    run_manifests = get_run_manifests( region='us-east-1', dynamo_db_table='snowplow-run-manifests')
    
    # TODO: Check that its ok to run for all run_ids
    all_paths = [enriched_events_archive + run_id for run_id in run_ids if not run_manifests.contains(run_id)]  
    run_spark_job(spark, all_paths , output_base_path)
    
    # for run_id in run_ids:
    #     if not run_manifests.contains(run_id):
    #         run_id_path = enriched_events_archive + run_id 
    #         run_spark_job(spark, run_id_path, output_base_path) # One job per run-id will be really slow
    #         run_manifests.add(run_id)

if __name__ == "__main__":
    # TODO: properly name command line arguments and add help text (e.g. --bucket)
    bucket = sys.argv[0]
    conf = SparkConf() # testing alt .setMaster('local[*]')
    spark = SparkSession.builder.appName(APP_NAME).config(conf=conf).getOrCreate()
    run_for_new_run_ids(spark, bucket)
    spark.stop()
