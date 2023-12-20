from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import Job
import sys
import boto3
import urllib.parse
import re
import time

boto_client = boto3.client('s3')

kwargs = {'Bucket': 'anilkumar-ift-598-iot-silver', 'Prefix': 'unprocessed'}

s3_list = []

while True:
    resp = boto_client.list_objects_v2(**kwargs)
    for content in resp.get('Contents', []):
        if content['Key'].endswith('.csv'):
            key = urllib.parse.unquote(content['Key'])
            s3_list.append('s3://' + kwargs['Bucket'] + '/' + key)
    try:
        kwargs['ContinuationToken'] = resp['NextContinuationToken']
    except KeyError:
        break

if len(s3_list) != 0:
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    # Specify S3 paths
    s3_input_path = "s3://anilkumar-ift-598-iot-silver/unprocessed/"
    s3_output_path = "s3://anilkumar-ift-598-iot-gold/cart-inventory/"

    df = spark.read.csv(s3_input_path, header=True)

    df.printSchema()  # Print the schema directly without using logger.info()

    # Write Spark DataFrame to Parquet with partitioning
    df.repartition(1).write.mode("append").partitionBy("creation_date").parquet(s3_output_path)
    # df.write.parquet(s3_output_path, mode='append', partitionBy=['creation_date'])

    for path in s3_list:
        match = re.match(r's3:\/\/(.+?)\/(.+)', path)
        src_bucket_name = match.group(1)
        src_key = match.group(2)
        copy_source = {
            'Bucket': src_bucket_name,
            'Key': src_key
        }
        print(copy_source)

        destination_key = "processed/" + '/'.join(src_key.split('/')[1:])
        destination_bucket = "anilkumar-ift-598-iot-silver"
        boto_client.copy(copy_source, destination_bucket, destination_key)
        boto_client.delete_object(Bucket=src_bucket_name, Key=src_key)

    athena = boto3.client('athena')
    response = athena.start_query_execution(
        QueryString=f'MSCK REPAIR TABLE cart_inventory;',
        QueryExecutionContext={
            'Database': "anilkumar-ift-598-iot"
        },
        ResultConfiguration={
            'OutputLocation': "s3://anilkumar-ift-598-iot-athena/_athena_results"
        }
    )
    execution_id = response['QueryExecutionId']
    state = 'RUNNING'

    while (state in ['RUNNING', 'QUEUED']):
        response = athena.get_query_execution(QueryExecutionId=execution_id)
        if 'QueryExecution' in response and 'Status' in response['QueryExecution'] and 'State' in \
                response['QueryExecution']['Status']:
            state = response['QueryExecution']['Status']['State']
            if state == 'FAILED':
                print(response)
                print("state == FAILED")
    
            elif state == 'SUCCEEDED':
                s3_path = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                filename = re.findall('.*\/(.*)', s3_path)[0]
                print(filename)
        time.sleep(1)

    # Job commit
    job.commit()


else:
    print("No Data to Process. Hence Skipping job")

