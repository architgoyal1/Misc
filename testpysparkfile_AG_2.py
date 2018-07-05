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

''

APP_NAME = "Snowplow Example"
DYNAMODB_RUN_MANIFESTS_TABLE = 'snowplow-run-manifests'
BUCKET = '<ENTER BUCKET HERE>'
ENRICHED_EVENTS_ARCHIVE = 's3://' + BUCKET + '/enriched/good/'
process_start_time = datetime.datetime.now() #time needs to be 24h (applies for every time in this script)

#Function to transform from hybrid to JSON using snowplowanalytics sdk
def snowplow_sdk(line):

    transform = snowplow_analytics_sdk.event_transformer.transform(line)

    return json.dumps(transform)


def main(sc, run_id, config, time_start, time_last):
    # Transform s3 files to simple JSON
    full_path = "s3a://" +  BUCKET + '/' + run_id + '*'
    transformed = sc.textFile(full_path).map(snowplow_sdk)

    # Import JSON to Spark Dataframe
    df = spark.read.json(transformed)

    #split dataframe by app_id (one for TEST and one for PROD)
    mask = df["app_id"] = "TEST"
    df_prod = df[~mask]
    df_test = df[mask]

    #For filename
    #populate from current time: format (YYYY-mm-DD hh:MM:SS)
    s = str(datetime.datetime.now())
    #numbers below depend on format
    YYYYMMDD = s[0:3] + s[5:6] + s[8:9]
    HHMMSS = s[11:12] + s[14:15] + s[17:18]
    date_time = YYYYMMDD + "-" + HHMMSS

    #write dataframe out into S3 [how??]
    df_prod.write.format('json').options(delimiter='|', header = 'false').save('<<enter s3 bucket name for new data here>>' +a+"SNP_"+a+"PROD"+a+"_"+a+ YYYYMMDD +a+"-"+a+ HHMMSS +a+"_df.json")
    df_test.write.format('json').options(delimiter='|', header = 'false').save('<<enter s3 bucket name for new data here>>' +a+"SNP_"+a+"TEST"+a+"_"+a+ YYYYMMDD +a+"-"+a+ HHMMSS +a+"_df.json")

#SPLIT FOR DF_TEST AND DF_PROD
#create summary of information
#count number of rows --> gives number of events
#assume etl timestamp is 'data time'; and 'time_start/end' are pyspark run times
    log_table_prod            =df_prod.agg( "SNP_PROD_"+date_time                   .alias("data_folder_name"),\
                                            count("*")                              .alias("number_of_events"),\
                                            max("etl_tstamp").astype('timestamp')   .alias("data_time_end")),\
                                            min("etl_tstamp").astype('timestamp')   .alias("data_time_start"),\
                                            process_start_time                      .alias("process_start_time"),\
                                            datetime.datetime.now()                 .alias("process_end_time"))

    log_table_test            =df_test.agg( "SNP_TEST_"+date_time                   .alias("data_folder_name"),\
                                            count("*")                              .alias("number_of_events"),\
                                            max("etl_tstamp").astype('timestamp')   .alias("data_time_end")),\
                                            min("etl_tstamp").astype('timestamp')   .alias("data_time_start"),\
                                            process_start_time                      .alias("process_start_time"),\
                                            datetime.datetime.now()                 .alias("process_end_time"))

#the next line of code is included in playwire script - not sure what it does?
###    log_table.write.jdbc(url, "logbook", mode="append", properties=properties)


if __name__ == "__main__":

    # Initialize run manifests
    session = boto3.Session()
    s3_client = session.client('s3')
    dynamodb_client = session.client('dynamodb', region_name='us-east-1') #VERIFY region
    run_manifests = RunManifests(dynamodb_client, DYNAMODB_RUN_MANIFESTS_TABLE)

    #why are these 2 the same? copied from snowplow run manifest documentation
    time_start = time.time()
    time_last  = time.time()


#Create table, add run to table and check if table contains ID --> PREVENTS REPROCESSING OF DATA
    run_ids = list_runids(s3_client, ENRICHED_EVENTS_ARCHIVE)
    for run_id in run_ids:
        if not run_manifests.contains(run_id):
            main(sc, run_id, cli_config, time_start, time_last)
            run_manifests.add(run_id)
        else:
            pass


#do we need to OPEN a .csv file before WRITING to it?
#Write logfile to csv
log_table_test.write.format('csv').options(delimiter='|', header = 'true').save('<<enter s3 bucket name for new data here>>' +a+"SNP_"+a+"TEST"+a+"_"+a+ YYYYMMDD +a+"-"+a+ HHMMSS +a+"_log.csv")
log_table_prod.write.format('csv').options(delimiter='|', header = 'true').save('<<enter s3 bucket name for new data here>>' +a+"SNP_"+a+"PROD"+a+"_"+a+ YYYYMMDD +a+"-"+a+ HHMMSS +a+"_log.csv")
