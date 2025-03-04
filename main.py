import os
import pika
import subprocess
import json
from minio import Minio
from minio.error import S3Error

# Read environment variables
RABBITMQ_URL = os.getenv('RABBITMQ_URL', 'amqp://user:password@localhost:5672/') #Connection string
QUEUE_NAME = os.getenv('RABBITMQ_QUEUE', 'task_queue')

MINIO_URL = os.getenv('MINIO_URL', 'localhost:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')


def parse_json_string(json_string):
    """Parses a JSON string and returns a Python object.

    Args:
        json_string: The JSON string to parse.

    Returns:
        A Python object (dict, list, etc.) representing the JSON data,
        or None if the string is not valid JSON.
    """
    try:
        data = json.loads(json_string)
        return data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


def download_file(run_id, file_name, extension):
    """
    Downloads a file from MinIO storage.

    Args:
        run_id (str): The run ID (directory) where the file is located.
        file_name (str): The name of the file.
        extension (str): The file extension.

    Returns:
        None. Prints success or error messages. Raises an exception on failure.
    """
    
    BUCKET_NAME = "code"

    try:
        # Initialize minio client object.
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,  # Set to True for HTTPS
        )

        # Construct object name and download path.
        object_name = f"{run_id}/{file_name}.{extension}"
        download_path = f"code/{run_id}/{file_name}.{extension}"

        # Ensure the directory exists
        os.makedirs(os.path.dirname(download_path), exist_ok=True)

        # Download the file.
        minio_client.fget_object(BUCKET_NAME, object_name, download_path)

        print(f"Successfully downloaded {object_name}")

    except S3Error as exc:
        print(f"Failed to download {object_name}: {exc}")
        raise exc #Reraise the exception so the caller knows it failed.
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise e


def process_message(ch, method, properties, body):
    """Callback function when a message is received from RabbitMQ"""
    
    msg = body.decode()
    print(f"Received Message: {msg}")

    data = parse_json_string(msg)
    
    if data is None:
        print("Invalid JSON. Ignoring message.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return
    
    runId = data.get("runId")
    fileName = data.get("fileName")
    extension = data.get("extension")

    try:
        download_file(runId, fileName, extension)
    except Exception as e:
        print(f"Download failed: {e}")

    
    abs_local_file = f"/code/{runId}/{fileName}.{extension}"
    parent_path = os.path.dirname(abs_local_file)
    # print(f"Running {abs_local_file}")

    try:
        # Get the directory of the parent script.
        parent_script_dir = os.path.dirname(os.path.abspath(__file__))

        # Construct the absolute path relative to the parent script's directory.
        full_file_path = os.path.join(parent_script_dir, abs_local_file.lstrip('/'))

        file_parent_dir = os.path.dirname(full_file_path)

        print("Running python -m scoop "+full_file_path)

        # Run the subprocess from the parent directory of the python script.
        result = subprocess.run(
            ['python','-m', 'scoop', full_file_path],
            capture_output=True,
            text=True,
            timeout=30,
            cwd=file_parent_dir  # Set the current working directory
        )
        output, error = result.stdout.strip(), result.stderr.strip()
        print(f"Output: {output}")
        print(f"Error: {error}")

    except Exception as e:
        print(f"Error running {fileName}: {e}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

# Connect to RabbitMQ using the connection string
parameters = pika.URLParameters(RABBITMQ_URL)
connection = pika.BlockingConnection(parameters)
channel = connection.channel()
channel.queue_declare(queue=QUEUE_NAME, durable=True)

print("Waiting for messages...")
channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_message)

try:
    channel.start_consuming()
except KeyboardInterrupt:
    print("Stopping worker...")
    connection.close()