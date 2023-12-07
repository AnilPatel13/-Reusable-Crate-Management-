class CONSTANT(object):
    DATE_FORMAT = '%Y-%m-%d'
    TIMESTAMP_FORMAT = '%Y-%m-%d %X'
    TIMESTAMP_IN_INT = '%Y%m%d%H%M%S'

class AWSCREDENTIALS:
    ACCESS_KEY = "XXXXXXXXX"
    SECRET_KEY = "XXXXXXXXX"
    REGION = "us-west-1"
    SQS_WAIT_QUEUE_URL = "https://sqs.us-west-1.amazonaws.com/050972822620/BufferQueue.fifo"
    S3_BUCKET_NAME = "anilkumar-ift-598-iot-bronze"
    ATHENA_S3_BUCKET = "s3://anilkumar-ift-598-iot-athena/"
    ATHENA_DATABASE = "anilkumar-ift-598-iot"
    ATHENA_TABLE = "cart_inventory"
    SQS_TOPIC_ARN = "arn:aws:sns:us-west-1:050972822620:IFT-598-IOT-Final-Anilkumar"

class PROJECT(object):
    PROJECT_NAME = "IOT"