import boto3
import os
from datetime import datetime


def lambda_handler(event, context):
    # Get the source and destination bucket names from environment variables
    source_bucket = os.environ['SOURCE_BUCKET']
    destination_bucket = os.environ['DESTINATION_BUCKET']
    sqs_queue_url = os.environ['SQS_QUEUE_URL']

    # Get the object key (file name) from the S3 event
    object_key = event['Records'][0]['s3']['object']['key']
    
    timestamp_str = event['Records'][0]['eventTime']
    
    # Parse the timestamp string
    timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Extract the date in 'YYYY-MM-DD' format
    formatted_date = timestamp.strftime('%Y-%m-%d')
    
    # Set the destination key (file name) in the destination bucket
    destination_key = "unprocessed/creation_date={0}/{1}".format(formatted_date, object_key.split('/')[-1])

    # Copy the object from the source bucket to the destination bucket
    s3 = boto3.client('s3')
    s3.copy_object(Bucket=destination_bucket, CopySource={'Bucket': source_bucket, 'Key': object_key}, Key=destination_key)

    # Delete the object from the source bucket
    s3.delete_object(Bucket=source_bucket, Key=object_key)

    print(f"File '{object_key}' moved from '{source_bucket}' to '{destination_bucket}'.")

    # Send a message to SQS for each uploaded file
    sqs = boto3.client('sqs')
    sqs.send_message(QueueUrl=sqs_queue_url, MessageBody=f"New file '{object_key}' uploaded to S3")

    # Delete messages from SQS if the queue count exceeds 2
    delete_messages_from_sqs(sqs_queue_url)

def delete_messages_from_sqs(queue_url):
    sqs = boto3.client('sqs')

    # Retrieve the message count in the SQS queue
    response = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['ApproximateNumberOfMessages']
    )
    
    print(response)
    
    message_count = int(response['Attributes']['ApproximateNumberOfMessages'])
    
    print(message_count)

    # If message count exceeds 1, delete messages
    if message_count > 10:
        print(f"Deleting messages from SQS. Message count in SQS: {message_count}")
        
        # Purge the entire SQS queue
        sqs.purge_queue(QueueUrl=queue_url)
        
        print(f"All messages deleted from SQS queue at {queue_url}")
        
        # Trigger aws Glue job
        glue = boto3.client('glue')
        
        job_name = "csvToParquet"
        
        # Trigger the Glue job
        response = glue.start_job_run(JobName=job_name)
    
        # Print the job run ID
        job_run_id = response['JobRunId']
        print(f"Glue job '{job_name}' triggered. Job Run ID: {job_run_id}")
    
