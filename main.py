from flask import Flask, render_template, request, redirect, url_for, session, flash
import json
import qrcode
from io import BytesIO, StringIO
import os
import random
from faker import Faker
from datetime import datetime, timedelta
from base64 import b64encode
import boto3
import csv
import time
from PIL import Image
from PIL import ImageDraw
from PIL import ImageFont
from luma.core.interface.serial import spi
from luma.oled.device import ssd1351
from helper.Constant import *
import cv2
import Adafruit_SSD1306
from PIL import Image, ImageDraw, ImageFont
import RPi.GPIO as GPIO

app = Flask(__name__)

app.secret_key = 'Tiger123'


@app.errorhandler(404)
def not_found(e):
    return render_template("404.html")


@app.route('/')
@app.route('/home_page', methods=["GET"])
def home_page():
    return render_template('index.html')


def generate_qr_code(data):
    # Create a QR code for the given data
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )
    qr.add_data(data)
    qr.make(fit=True)

    # Create an image from the QR Code instance
    img = qr.make_image(fill_color="black", back_color="white")

    # Save the image to a BytesIO object
    image_bytes = BytesIO()
    img.save(image_bytes, "PNG")

    return image_bytes.getvalue()


@app.route('/print_qr_code', methods=["POST", "GET"])
def print_qr_code():
    if request.method == "POST":
        crate_id = request.form.get("crate_id")
        customer_id = request.form.get("customer_id")
        customer_name = request.form.get("customer_name")
        delivery_address = request.form.get("delivery_address")
        city = request.form.get("delivery_city")
        state = request.form.get("state")
        delivery_date = request.form.get("delivery_date")
        delivery_time = request.form.get("delivery_time")
        return_date = request.form.get("return_date")
        return_time = request.form.get("return_time")
        contents = request.form.get("contents")
        batch_order_number = request.form.get("batch_order_number")
        transportation_details = request.form.get("transportation_details")
        return_status = request.form.get("return_status")
        qr_code_generation_date = request.form.get("qr_code_generation_date")
        additional_notes = request.form.get("additional_notes")
        temperature = request.form.get("temperature")
        weight = request.form.get("weight")
        expiration_date = request.form.get("expiration_date")
        handling_instructions = request.form.get("handling_instructions")
        shipment_status = request.form.get("shipment_status")
        company_name = request.form.get("company_name")
        creation_date = request.form.get("creation_date")

        # Create a dictionary
        data_dict = {
            "crate_id": crate_id,
            "customer_id": customer_id,
            "customer_name": customer_name,
            "delivery_address": delivery_address,
            "city": city,
            "state": state,
            "delivery_date": delivery_date,
            "delivery_time": delivery_time,
            "return_date": return_date,
            "return_time": return_time,
            "contents": contents,
            "batch_order_number": batch_order_number,
            "transportation_details": transportation_details,
            "return_status": return_status,
            "qr_code_generation_date": qr_code_generation_date,
            "additional_notes": additional_notes,
            "temperature": temperature,
            "weight": weight,
            "expiration_date": expiration_date,
            "handling_instructions": handling_instructions,
            "shipment_status": shipment_status,
            "company_name": company_name,
            "creation_date": creation_date,
        }

        # Convert the dictionary to JSON
        json_data = json.dumps(data_dict, indent=2)

        sqs = boto3.client('sqs', aws_access_key_id=AWSCREDENTIALS.ACCESS_KEY,
                           aws_secret_access_key=AWSCREDENTIALS.SECRET_KEY,
                           region_name=AWSCREDENTIALS.REGION)
        # Send the JSON data to the FIFO queue
        response = sqs.send_message(
            QueueUrl=AWSCREDENTIALS.SQS_WAIT_QUEUE_URL,
            MessageBody=json_data,
            MessageGroupId='your-message-group-id',  # Specify a unique identifier for the message group
            MessageDeduplicationId=str(hash(json_data))  # Use a unique identifier to deduplicate messages
        )
        # Check if the message was successfully sent
        if 'MessageId' in response:
            flash("Successfully Printed Qr Code | Crate Id: {}".format(crate_id), "success")
        else:
            flash("Failed to Printed Qr Code | Crate Id: {}".format(crate_id), "danger")

        fake = Faker()

        # List of cities in Arizona
        arizona_cities = [
            'Phoenix', 'Tucson', 'Mesa', 'Chandler', 'Gilbert', 'Glendale', 'Scottsdale',
            'Tempe',
            'Flagstaff'
        ]
        data = []
        company_names = [
            'Sysco Corporation',
            'US Foods',
            'Performance Food Group',
            'Gordon Food Service',
            'McLane Company',
            'Reinhart Foodservice',
            'Ben E. Keith Company',
            'Martin Brower',
            'Cheney Brothers',
            'Shamrock Foods Company'
        ]

        handling_instructions = ['Store in a cool place', 'Handle with care']
        additional_notes = ['Handle with care', 'Fragile', 'Check for the expiration date']
        contents = ['Canned Goods, Pasta', 'Dairy Products', 'Fruits and Vegetables']

        today = datetime.now()
        one_week_later = today + timedelta(days=7)

        # Generate a single record
        city = random.choice(arizona_cities)

        record = {
            'crate_id': random.randint(100000, 999999),
            'customer_id': 'CUST' + str(random.randint(1, 1000)).zfill(3),
            'customer_name': fake.name(),
            'delivery_address': fake.street_address(),
            'city': city,
            'state': 'Arizona',
            'delivery_date': today.strftime('%Y-%m-%d'),
            'delivery_time': fake.time(),
            'return_date': one_week_later.strftime('%Y-%m-%d'),
            'return_time': fake.time(),
            'contents': random.choice(contents),
            'batch_order_number': 'ORD' + str(random.randint(1, 100)).zfill(3),
            'transportation_details': random.choice(['Truck - Refrigerated', 'Ship - Dry Cargo']),
            'return_status': random.choice(['Returned', 'In Transit', 'Delivered']),
            'qr_code_generation_date': today.strftime('%Y-%m-%d'),
            'additional_notes': random.choice(additional_notes),
            'temperature': f"{random.uniform(0, 10):.2f} C",
            'weight': f"{random.uniform(100, 500):.2f} kg",
            'expiration_date': one_week_later.strftime('%Y-%m-%d'),
            'handling_instructions': random.choice(handling_instructions),
            'shipment_status': random.choice(['Delivered', 'In Transit']),
            'company_name': random.choice(company_names),
            'creation_date': today.strftime('%Y-%m-%d'),
        }
        data.append(record)
        # Convert record to JSON
        json_data = json.dumps(record, indent=2)

        # print(json_data)

        # Generate QR code
        qr_code_data = generate_qr_code(json_data)

        decoded_qr_code = b64encode(qr_code_data).decode("utf-8")

        return render_template('print_qr_code.html', data=data, qr_code_data=decoded_qr_code)

    elif request.method == "GET":
        fake = Faker()

        # List of cities in Arizona
        arizona_cities = [
            'Phoenix', 'Tucson', 'Mesa', 'Chandler', 'Gilbert', 'Glendale', 'Scottsdale',
            'Tempe',
            'Flagstaff'
        ]
        data = []
        company_names = [
            'Sysco Corporation',
            'US Foods',
            'Performance Food Group',
            'Gordon Food Service',
            'McLane Company',
            'Reinhart Foodservice',
            'Ben E. Keith Company',
            'Martin Brower',
            'Cheney Brothers',
            'Shamrock Foods Company'
        ]

        handling_instructions = ['Store in a cool place', 'Handle with care']
        additional_notes = ['Handle with care', 'Fragile', 'Check for the expiration date']
        contents = ['Canned Goods, Pasta', 'Dairy Products', 'Fruits and Vegetables']

        today = datetime.now()
        one_week_later = today + timedelta(days=7)

        # Generate a single record
        city = random.choice(arizona_cities)

        record = {
            'crate_id': random.randint(100000, 999999),
            'customer_id': 'CUST' + str(random.randint(1, 1000)).zfill(3),
            'customer_name': fake.name(),
            'delivery_address': fake.street_address(),
            'city': city,
            'state': 'Arizona',
            'delivery_date': today.strftime('%Y-%m-%d'),
            'delivery_time': fake.time(),
            'return_date': one_week_later.strftime('%Y-%m-%d'),
            'return_time': fake.time(),
            'contents': random.choice(contents),
            'batch_order_number': 'ORD' + str(random.randint(1, 100)).zfill(3),
            'transportation_details': random.choice(['Truck - Refrigerated', 'Ship - Dry Cargo']),
            'return_status': random.choice(['Returned', 'In Transit', 'Delivered']),
            'qr_code_generation_date': today.strftime('%Y-%m-%d'),
            'additional_notes': random.choice(additional_notes),
            'temperature': f"{random.uniform(0, 10):.2f} C",
            'weight': f"{random.uniform(100, 500):.2f} kg",
            'expiration_date': one_week_later.strftime('%Y-%m-%d'),
            'handling_instructions': random.choice(handling_instructions),
            'shipment_status': random.choice(['Delivered', 'In Transit']),
            'company_name': random.choice(company_names),
            'creation_date': today.strftime('%Y-%m-%d'),
        }
        data.append(record)
        # Convert record to JSON
        json_data = json.dumps(record, indent=2)

        # print(json_data)

        # Generate QR code
        qr_code_data = generate_qr_code(json_data)

        decoded_qr_code = b64encode(qr_code_data).decode("utf-8")

        return render_template('print_qr_code.html', data=data, qr_code_data=decoded_qr_code)
    else:
        return render_template("404.html")


# Function to convert JSON data to CSV format
def convert_json_to_csv(json_data):
    csv_data = StringIO()
    csv_writer = csv.DictWriter(csv_data, fieldnames=json_data.keys())
    csv_writer.writeheader()
    csv_writer.writerow(json_data)
    csv_data.seek(0)
    return csv_data.getvalue()


@app.route('/scan_qr_code', methods=["POST", "GET"])
def scan_qr_code():
    if request.method == "POST":

        # Constants for LED pins
        led1_pin = 18
        led2_pin = 23

        GPIO.setmode(GPIO.BCM)
        GPIO.setwarnings(False)
        GPIO.setup(led1_pin, GPIO.OUT, initial=GPIO.LOW)
        GPIO.setup(led2_pin, GPIO.OUT, initial=GPIO.LOW)
        GPIO.output(led1_pin, GPIO.HIGH)

        display = Adafruit_SSD1306.SSD1306_128_64(rst=None)
        display.begin()
        display.clear()
        display.display()
        width, height = display.width, display.height
        image = Image.new('1', (width, height))
        draw = ImageDraw.Draw(image)
        font = ImageFont.load_default()

        # Display "Scanning in progress!" in yellow
        draw.text((2, 2), "\n                 scanning \n             in progress!", font=font, fill=1)
        display.image(image)
        display.display()

        message_id = request.form.get('message_id')
        json_data_str = request.form.get('json_data')

        json_data = json.loads(json_data_str)

        # Raspberry Pi SPI Configuration
        serial = spi(port=0, device=0, gpio_DC=25, gpio_RST=27)

        # OLED Configuration
        OLED_WIDTH = 128
        OLED_HEIGHT = 128

        # Initialize display
        disp = ssd1351(serial, width=OLED_WIDTH, height=OLED_HEIGHT)

        # Create image buffer
        image = Image.new('RGB', (OLED_WIDTH, OLED_HEIGHT), 'white')
        draw = ImageDraw.Draw(image)

        # Load default font
        font = ImageFont.load_default()

        qr = qrcode.QRCode(
            version=1,
            error_correction=qrcode.constants.ERROR_CORRECT_L,
            box_size=2.8,  # Adjust the box size to control QR code size
            border=4,
        )

        new_json_data = {}
        new_json_data['crate_id'] = json_data.get('crate_id', None)
        new_json_data['customer_id'] = json_data.get('customer_id', None)
        new_json_data['customer_name'] = json_data.get('customer_name', None)
        # new_json_data['city'] = json_data.get('city', None)
        # new_json_data['state'] = json_data.get('state', None)

        new_json_data_str = json.dumps(new_json_data, indent=2)

        qr.add_data(new_json_data_str)
        qr.make(fit=True)

        qr_image = qr.make_image(fill_color="white", back_color="black")
        qr_size = qr_image.size

        # Calculate position to center the QR code on the OLED
        x = (OLED_WIDTH - qr_size[0]) // 2
        y = (OLED_HEIGHT - qr_size[1]) // 2

        # Paste the QR code onto the image
        image.paste(qr_image, (x, y))

        # Display the image on the OLED
        disp.display(image)

        # set up camera object
        cap = cv2.VideoCapture(0)

        # QR code detection object
        detector = cv2.QRCodeDetector()

        while True:
            # get the image
            _, img = cap.read()
            # img = cv2.resize(img, (1024, 640))
            # get bounding box coords and data
            data, bbox, _ = detector.detectAndDecode(img)

            # if there is a bounding box, draw one, along with the data
            if (bbox is not None):
                bbox = bbox[0].astype(int)  # Convert to integers
                for i in range(len(bbox)):
                    pt1 = tuple(bbox[i])
                    pt2 = tuple(bbox[(i + 1) % len(bbox)])
                    cv2.line(img, pt1, pt2, color=(255, 0, 255), thickness=2)

                cv2.putText(img, data, (int(bbox[0][0]), int(bbox[0][1]) - 10), cv2.FONT_HERSHEY_SIMPLEX,
                            0.5, (0, 255, 0), 2)
                if data:
                    print("data found: ", data)
                    break

            # display the image preview
            cv2.imshow("code detector", img)

            # Break the loop when 'q' key is pressed
            if cv2.waitKey(1) & 0xFF == ord('q'):
                break

        # Release the camera and close the window
        cap.release()
        cv2.destroyAllWindows()

        # Initialize AWS clients
        s3 = boto3.client('s3', aws_access_key_id=AWSCREDENTIALS.ACCESS_KEY,
                          aws_secret_access_key=AWSCREDENTIALS.SECRET_KEY,
                          region_name=AWSCREDENTIALS.REGION)
        sqs = boto3.client('sqs', aws_access_key_id=AWSCREDENTIALS.ACCESS_KEY,
                           aws_secret_access_key=AWSCREDENTIALS.SECRET_KEY,
                           region_name=AWSCREDENTIALS.REGION)

        # Convert JSON data to CSV
        csv_data_str = convert_json_to_csv(json_data)

        # Generate a unique file name using the current timestamp
        file_name = f"raw/csv_data_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"

        # Encode CSV data to UTF-8 before uploading
        csv_data_bytes = csv_data_str.encode('utf-8')

        # Upload CSV file to S3
        s3.upload_fileobj(BytesIO(csv_data_bytes), AWSCREDENTIALS.S3_BUCKET_NAME, file_name)

        # Delete the message from SQS
        response = sqs.receive_message(
            QueueUrl=AWSCREDENTIALS.SQS_WAIT_QUEUE_URL,
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=1,
            VisibilityTimeout=60,
            WaitTimeSeconds=0,
        )

        if 'Messages' in response:
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']

            # Check if the message ID matches the desired message to delete
            if message['MessageId'] == message_id:
                # Use delete_message to delete the message by providing the receipt handle
                sqs.delete_message(
                    QueueUrl=AWSCREDENTIALS.SQS_WAIT_QUEUE_URL,
                    ReceiptHandle=receipt_handle
                )
                print(f"Message with ID {message_id} deleted.")
            else:
                print(f"Message with ID {message_id} not found.")
        else:
            print("No messages found in the queue.")

        print(f"CSV file uploaded to S3: {file_name}")

        # Clear the display
        disp.clear()
        #
        # Clean up resources
        disp.cleanup()

        GPIO.output(led2_pin, GPIO.HIGH)

        # Clear the display
        display.begin()
        display.clear()
        display.display()

        # Create a new image
        width, height = display.width, display.height
        image = Image.new('1', (width, height))
        draw = ImageDraw.Draw(image)

        # Display "Scanning Completed!" in white
        draw.text((1, 1), "\n                 scanning \n              Completed!", font=font, fill=255)
        display.image(image)
        display.display()

        time.sleep(5)

        sns = boto3.client('sns', aws_access_key_id=AWSCREDENTIALS.ACCESS_KEY,
                           aws_secret_access_key=AWSCREDENTIALS.SECRET_KEY,
                           region_name=AWSCREDENTIALS.REGION)

        topic_arn = AWSCREDENTIALS.SQS_TOPIC_ARN

        subject = 'Anilkumar IOT Final - Scan Completed'

        message = 'QR Code Scan Completed for Crate id: {0}'.format(new_json_data['crate_id'])

        # Publish the message to the SNS topic
        response = sns.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=message
        )

        # Check the response for any errors
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            print(f"Email Message sent successfully to {topic_arn}")
        else:
            print("Failed to send the Email message")
            print(response)

        # Clear the display
        display.begin()
        display.clear()
        display.display()

        # Create a new image
        width, height = display.width, display.height
        image = Image.new('1', (width, height))
        draw = ImageDraw.Draw(image)

        # Display "Scanning Completed!" in white
        draw.text((1, 1), "\n     Email Message Sent!", font=font, fill=255)
        display.image(image)
        display.display()

        time.sleep(3)

        GPIO.output(led1_pin, GPIO.LOW)
        GPIO.output(led2_pin, GPIO.LOW)

        display.begin()
        display.clear()
        display.display()

        return redirect(url_for('scan_qr_code'))


    elif request.method == "GET":
        sqs = boto3.client('sqs', aws_access_key_id=AWSCREDENTIALS.ACCESS_KEY,
                           aws_secret_access_key=AWSCREDENTIALS.SECRET_KEY,
                           region_name=AWSCREDENTIALS.REGION)
        # Replace 'your-queue-url' with the URL of your SQS queue
        queue_url = AWSCREDENTIALS.SQS_WAIT_QUEUE_URL

        # Set the maximum number of messages to retrieve
        max_messages = 1

        # Receive messages from the queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_messages,
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )

        if 'Messages' in response:
            messages = response['Messages']
            message_id = messages[0]['MessageId']
            message_body = json.loads(messages[0]['Body'])
            json_data = json.dumps(message_body, indent=2)

            new_json_data = {}
            new_json_data['crate_id'] = message_body.get('crate_id', None)
            new_json_data['customer_id'] = message_body.get('customer_id', None)
            new_json_data['customer_name'] = message_body.get('customer_name', None)
            # new_json_data['city'] = message_body.get('city', None)
            # new_json_data['state'] = message_body.get('state', None)

            json_data_str = json.dumps(new_json_data, indent=2)

            qr_code_data = generate_qr_code(json_data_str)

            decoded_qr_code = b64encode(qr_code_data).decode("utf-8")
            return render_template('scan_qr_code.html', qr_code_data=decoded_qr_code, message_id=message_id,
                                   json_data=json_data)
        else:
            flash("No More Messages to Processed or Scanned", "danger")
            return render_template('scan_qr_code.html')

    else:
        return render_template("404.html")


@app.route('/inprogress_queue', methods=["GET"])
def inprogress_queue():
    if request.method == "GET":
        sqs = boto3.client('sqs', aws_access_key_id=AWSCREDENTIALS.ACCESS_KEY,
                           aws_secret_access_key=AWSCREDENTIALS.SECRET_KEY,
                           region_name=AWSCREDENTIALS.REGION)

        # Replace 'your-queue-url' with the URL of your SQS queue
        queue_url = AWSCREDENTIALS.SQS_WAIT_QUEUE_URL

        # Set the maximum number of messages to retrieve
        max_messages = 10

        # Receive messages from the queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MessageAttributeNames=['All'],
            MaxNumberOfMessages=max_messages,
            VisibilityTimeout=0,
            WaitTimeSeconds=0
        )

        data_fetch = []

        # Check if there are messages in the response
        if 'Messages' in response:
            messages = response['Messages']

            # Process each message
            for message in messages:
                temp_dict = {}
                # Extract and process the message body (assuming it's JSON)
                message_body = json.loads(message['Body'])
                temp_dict['crate_id'] = message_body['crate_id']
                temp_dict['customer_id'] = message_body['customer_id']
                temp_dict['customer_name'] = message_body['customer_name']
                temp_dict['delivery_address'] = message_body['delivery_address']
                temp_dict['city'] = message_body['city']
                temp_dict['state'] = message_body['state']
                temp_dict['delivery_date'] = message_body['delivery_date']
                temp_dict['delivery_time'] = message_body['delivery_time']
                temp_dict['return_date'] = message_body['return_date']
                temp_dict['return_time'] = message_body['return_time']
                temp_dict['contents'] = message_body['contents']
                temp_dict['company_name'] = message_body['company_name']
                data_fetch.append(temp_dict)
        else:
            print("No messages in the queue.")
        return render_template('inprogress_queue.html', data_fetch=data_fetch)
    else:
        return render_template("404.html")


@app.route('/crate_tracking', methods=["GET"])
def crate_tracking():
    if request.method == "GET":
        athena_client = boto3.client('athena', aws_access_key_id=AWSCREDENTIALS.ACCESS_KEY,
                                     aws_secret_access_key=AWSCREDENTIALS.SECRET_KEY,
                                     region_name=AWSCREDENTIALS.REGION)
        query = """SELECT crate_id,customer_name, company_name, city, state, return_date, contents,
                transportation_details, shipment_status FROM {0} where shipment_status = 'Delivered' """ \
            .format(AWSCREDENTIALS.ATHENA_TABLE)

        # Execution
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': AWSCREDENTIALS.ATHENA_DATABASE
            },
            ResultConfiguration={
                'OutputLocation': AWSCREDENTIALS.ATHENA_S3_BUCKET,  # Adjust your S3 bucket
            }
        )

        query_execution_id = response['QueryExecutionId']

        while True:
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']

            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break

            print(f"Current status: {status}")
            time.sleep(2)  # Adjust sleep time as needed

        if status == 'FAILED' or status == 'CANCELLED':
            print(f"Query {query_execution_id} did not succeed: {status}")
            print(f"Reason: {response['QueryExecution']['Status']['StateChangeReason']}")
        else:
            # Get the results
            results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

            # Convert results to DataFrame
            columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            rows = results['ResultSet']['Rows'][1:]  # Skip the header row

            # Create a list of dictionaries
            data_fetch = []
            for row in rows:
                data_row = {}
                for i, datum in enumerate(row['Data']):
                    data_row[columns[i]] = datum['VarCharValue']
                data_fetch.append(data_row)

        return render_template('crate_tracking.html', data_fetch=data_fetch)
    else:
        return render_template("404.html")


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
