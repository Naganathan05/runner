import os
import pika
import subprocess
import json
from minio import Minio
from minio.error import S3Error
import psycopg2
import threading
import queue
import websocket
import time

# Read environment variables
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://user:password@localhost:5672/")
QUEUE_NAME = os.getenv("RABBITMQ_QUEUE", "task_queue")
MINIO_URL = os.getenv("MINIO_URL", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
COCKROACHDB_URL = os.getenv(
    "COCKROACHDB_URL", "postgresql://root@localhost:26257/defaultdb?sslmode=disable"
)
WEBSOCKET_LOG_URL = os.getenv("WEBSOCKET_LOG_URL", "ws://localhost:5002/live")


def parse_json_string(json_string):
    """Parses a JSON string and returns a Python object."""
    try:
        data = json.loads(json_string)
        return data
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return None


def download_file(run_id, file_name, extension):
    """Downloads a file from MinIO storage."""
    BUCKET_NAME = "code"
    try:
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        object_name = f"{run_id}/{file_name}.{extension}"
        download_path = f"code/{run_id}/{file_name}.{extension}"
        os.makedirs(os.path.dirname(download_path), exist_ok=True)
        minio_client.fget_object(BUCKET_NAME, object_name, download_path)
        print(f"Successfully downloaded {object_name} to {download_path}")
        return os.path.abspath(download_path)
    except S3Error as exc:
        print(f"Failed to download {object_name}: {exc}")
        raise exc
    except Exception as e:
        print(f"An unexpected error occurred during download: {e}")
        raise e


def upload_file(run_id, file_path):
    """Uploads a file to MinIO storage."""
    BUCKET_NAME = "code"
    try:
        minio_client = Minio(
            MINIO_URL,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False,
        )
        # Extract filename from the full path.
        file_name_with_ext = os.path.basename(file_path)
        object_name = f"{run_id}/{file_name_with_ext}"
        minio_client.fput_object(BUCKET_NAME, object_name, file_path)
        print(f"Successfully uploaded {file_path} as {object_name}")
    except S3Error as exc:
        print(f"Failed to upload {file_path}: {exc}")
        raise exc
    except Exception as e:
        print(f"An unexpected error occurred during upload: {e}")
        raise e


# --- Helper Functions for Streaming ---

# Sentinel object to signal the end of a stream
STREAM_END = object()


def stream_reader(stream, stream_name, log_queue):
    """Reads lines from a stream and puts them onto the queue."""
    try:
        # Use iter to read lines efficiently
        for line in iter(stream.readline, b""):
            try:
                decoded_line = line.decode("utf-8").rstrip()
                log_entry = json.dumps({"stream": stream_name, "line": decoded_line})
                log_queue.put(log_entry)
            except UnicodeDecodeError:
                # Handle cases where output might not be valid UTF-8
                log_entry = json.dumps(
                    {"stream": stream_name, "line": repr(line)[2:-1]}
                )  # Use repr for safety
                log_queue.put(log_entry)
            except Exception as e:
                print(f"Error processing log line from {stream_name}: {e}")
        print(f"Stream reader for {stream_name} finished.")
        final_status_message = "__END__"
        try:
            log_queue.put(final_status_message)
            print(f"Final status sent to WebSocket: {final_status_message}")
        except Exception as e:
            print(f"Error sending final status to WebSocket: {e}")
    except Exception as e:
        print(f"Error in stream reader for {stream_name}: {e}")
    finally:
        # Signal that this stream is finished
        log_queue.put(STREAM_END)
        if stream:
            stream.close()


def log_sender(ws_url, log_queue):
    """Connects to WebSocket and sends logs from the queue."""
    ws = None
    active_streams = 2  # Start with 2 (stdout, stderr)
    connection_attempts = 0
    max_attempts = 5
    retry_delay = 2  # seconds

    while connection_attempts < max_attempts:
        try:
            print(f"Attempting to connect to WebSocket: {ws_url}")
            ws = websocket.create_connection(ws_url, timeout=10)
            print(f"WebSocket connection established to {ws_url}")
            break
        except (
            websocket.WebSocketException,
            ConnectionRefusedError,
            TimeoutError,
            OSError,
        ) as e:
            connection_attempts += 1
            print(
                f"WebSocket connection failed (Attempt {connection_attempts}/{max_attempts}): {e}"
            )
            if connection_attempts >= max_attempts:
                print(
                    "Max connection attempts reached. Cannot send logs via WebSocket."
                )
                # Drain the queue to prevent blocking reader threads indefinitely
                # if they are still producing output (though they should stop soon)
                while active_streams > 0:
                    item = log_queue.get()
                    if item is STREAM_END:
                        active_streams -= 1
                    log_queue.task_done()
                return
            print(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay *= 2  # Exponential backoff
        except Exception as e:
            print(f"Unexpected error during WebSocket connection: {e}")
            return

    try:
        while active_streams > 0:
            try:
                # Use timeout to prevent blocking indefinitely if queue becomes empty unexpectedly.
                log_entry = log_queue.get(timeout=150)
            except queue.Empty:
                print("Log queue empty timeout reached. Checking connection/streams.")
                if ws and not ws.connected:
                    print("WebSocket disconnected unexpectedly.")
                    break
                continue

            if log_entry is STREAM_END:
                active_streams -= 1
                print(
                    f"Received end signal. Active streams remaining: {active_streams}"
                )
            else:
                try:
                    if ws and ws.connected:
                        # print(f"Sending log: {log_entry}") # Uncomment for debug
                        ws.send(log_entry)
                    else:
                        print("Cannot send log, WebSocket is not connected.")
                except websocket.WebSocketException as e:
                    print(f"Error sending log via WebSocket: {e}")
                    # Handle disconnection or other send errors.
                    break
                except Exception as e:
                    print(f"Unexpected error sending log: {e}")
                    break  # Stop sending on unexpected errors.

            log_queue.task_done()  # Mark task as done for queue management.

    except Exception as e:
        print(f"Error in log sender loop: {e}")
    finally:
        print("Log sender thread finishing.")
        if ws and ws.connected:
            try:
                ws.close()
                print("WebSocket connection closed.")
            except Exception as e:
                print(f"Error closing WebSocket: {e}")


# Callback function that is called when a message is received.
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

    if not all([runId, fileName, extension]):
        print("Missing runId, fileName, or extension in message. Ignoring.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # --- WebSocket Setup ---
    ws_thread = None
    log_queue = queue.Queue()
    ws_url_for_run = None
    if WEBSOCKET_LOG_URL:
        ws_url_for_run = f"{WEBSOCKET_LOG_URL.rstrip('/')}/{runId}"
        # Start the log sender thread
        ws_thread = threading.Thread(
            target=log_sender, args=(ws_url_for_run, log_queue), daemon=True
        )
        ws_thread.start()
    else:
        print("WEBSOCKET_LOG_URL not set. Skipping live log streaming.")
    # --- End WebSocket Setup ---

    local_file_path = None
    process = None
    stdout_thread = None
    stderr_thread = None
    run_status = "failed"

    try:
        # --- Download ---
        local_file_path = download_file(runId, fileName, extension)
        file_parent_dir = os.path.dirname(local_file_path)
        print(f"Code downloaded to: {local_file_path}")
        print(f"Parent directory: {file_parent_dir}")

        # --- Update Status to Running & Get Type ---
        runType = "scoop"
        try:
            conn = psycopg2.connect(COCKROACHDB_URL)
            cur = conn.cursor()
            cur.execute("UPDATE run SET status = 'running' WHERE id = %s", (runId,))
            cur.execute("SELECT type FROM run WHERE id = %s", (runId,))
            result = cur.fetchone()
            if result:
                runType = result[0]
                print(f"Run type: {runType}")
            else:
                print(f"Warning: Could not fetch run type for {runId}")
            conn.commit()
            print(f"Run {runId} status updated to 'running'.")
            run_status = "running"
        except psycopg2.Error as e:
            print(f"Error updating run status/fetching type in CockroachDB: {e}")
            raise
        finally:
            if conn:
                cur.close()
                conn.close()

        # --- Execute Subprocess ---
        command = []

        if runType == "ml":
            command = ["python", local_file_path]
        else:
            command = ["python", "-m", "scoop", local_file_path]

        # command = ["python", "-m", "scoop", local_file_path]
        timeout_sec = 3000  # Set timeout for scoop

        print(f"Running command: {' '.join(command)} in {file_parent_dir}")
        print(f"Timeout: {timeout_sec} seconds")

        # Acknowledge message BEFORE starting the potentially long subprocess
        print("Acknowledging RabbitMQ message before starting subprocess.")
        ch.basic_ack(delivery_tag=method.delivery_tag)

        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=file_parent_dir,
        )

        # Start reader threads if WebSocket is enabled
        if ws_thread:
            stdout_thread = threading.Thread(
                target=stream_reader,
                args=(process.stdout, "stdout", log_queue),
                daemon=True,
            )
            stderr_thread = threading.Thread(
                target=stream_reader,
                args=(process.stderr, "stderr", log_queue),
                daemon=True,
            )
            stdout_thread.start()
            stderr_thread.start()
        else:
            # If not streaming, we might still want to capture output later
            # Or just let it go if not needed
            pass

        # --- Wait for Process Completion ---
        try:
            print(f"Waiting for subprocess (PID: {process.pid}) to complete...")
            return_code = process.wait(timeout=timeout_sec)
            print(f"Subprocess finished with return code: {return_code}")
            if return_code == 0:
                run_status = "completed"
            else:
                run_status = "failed"  # Mark as failed if non-zero exit code

        except subprocess.TimeoutExpired:
            print(f"Subprocess timed out after {timeout_sec} seconds. Terminating...")
            # Send SIGTERM first, then SIGKILL if necessary
            # Killing the process group might be needed if using setsid/preexec_fn=os.setsid
            # os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            process.terminate()
            try:
                process.wait(timeout=5)  # Wait a bit for termination
            except subprocess.TimeoutExpired:
                print("Process did not terminate gracefully. Sending SIGKILL...")
                process.kill()
                process.wait()  # Wait for kill
            run_status = "timed_out"
            print("Subprocess terminated due to timeout.")
        except Exception as e:
            # Catch other potential errors during wait
            print(f"Error waiting for subprocess: {e}")
            run_status = "failed"
            if process.poll() is None:
                process.kill()
                process.wait()

        # --- Wait for Log Streaming to Finish ---
        # Ensure reader threads finish processing any remaining output
        # The readers will put STREAM_END onto the queue when their stream closes
        if stdout_thread:
            stdout_thread.join(timeout=10)
        if stderr_thread:
            stderr_thread.join(timeout=10)

        # Wait for the sender thread to send all queued logs
        if ws_thread:
            print("Waiting for log sender thread to finish...")
            ws_thread.join(timeout=3000)  # Wait for sender (adjust timeout)
            if ws_thread.is_alive():
                print("Warning: Log sender thread did not finish in time.")

        # --- Upload Results ---
        print(f"Uploading results from {file_parent_dir} for run {runId}...")
        uploaded_files = []
        if os.path.exists(file_parent_dir):
            for file in os.listdir(file_parent_dir):
                # Include common output types, maybe configure this list
                if file.endswith(
                    (".txt", ".png", ".gif", ".log", ".csv", ".json", ".pkl")
                ):
                    file_to_upload = os.path.join(file_parent_dir, file)
                    if os.path.isfile(file_to_upload):  # Ensure it's a file
                        try:
                            upload_file(runId, file_to_upload)
                            uploaded_files.append(file)
                        except Exception as e:
                            print(f"Individual file upload failed for {file}: {e}")
            print(f"Uploaded files: {uploaded_files if uploaded_files else 'None'}")
        else:
            print(f"Directory {file_parent_dir} does not exist, cannot upload results.")

        # --- Final Status Update ---
        try:
            conn = psycopg2.connect(COCKROACHDB_URL)
            cur = conn.cursor()
            print(f"Updating run {runId} final status to '{run_status}'.")
            cur.execute("UPDATE run SET status = %s WHERE id = %s", (run_status, runId))
            conn.commit()
        except psycopg2.Error as e:
            print(f"Error updating final run status in CockroachDB: {e}")
            # Log error, but don't necessarily crash the worker
        finally:
            if conn:
                cur.close()
                conn.close()

    except Exception as e:
        print(f"!! Critical error processing message for run {runId}: {e}")
        # Ensure process is killed if it's still running
        if process and process.poll() is None:
            print("Killing subprocess due to critical error.")
            process.kill()
            process.wait()
        run_status = "failed"  # Ensure status is marked as failed
        # Update DB status to failed if it wasn't already set
        try:
            conn = psycopg2.connect(COCKROACHDB_URL)
            cur = conn.cursor()
            # Check current status before overriding? Maybe not, failure overrides.
            print(f"Updating run {runId} status to 'failed' due to critical error.")
            cur.execute("UPDATE run SET status = 'failed' WHERE id = %s", (runId,))
            conn.commit()
        except psycopg2.Error as db_e:
            print(f"Error updating run status to failed in CockroachDB: {db_e}")
        finally:
            if conn:
                cur.close()
                conn.close()

        # NACK the message if we haven't ACKed it yet.
        # In this revised code, we ACK *before* the subprocess starts.
        # If a failure happens *before* the ACK, we might want to NACK.
        # However, the current logic ACK's early. If the goal is retries
        # on failure, the ACK point needs reconsideration.
        # For now, we assume early ACK means "don't retry this message".
        # ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False) # Example if ACK was later

    finally:
        # --- Cleanup ---
        # Ensure threads are joined (redundant if already joined, but safe)
        if stdout_thread and stdout_thread.is_alive():
            stdout_thread.join(timeout=1)
        if stderr_thread and stderr_thread.is_alive():
            stderr_thread.join(timeout=1)
        if ws_thread and ws_thread.is_alive():
            ws_thread.join(timeout=1)

        # TODO: Add logic to delete the local run directory if desired
        # Be cautious if multiple workers might access shared directories
        # if local_file_path:
        #     run_dir_to_delete = os.path.dirname(local_file_path)
        #     try:
        #         print(f"Attempting to remove directory: {run_dir_to_delete}")
        #         # shutil.rmtree(run_dir_to_delete)
        #         print(f"Directory {run_dir_to_delete} removed.")
        #     except Exception as clean_e:
        #         print(f"Error removing directory {run_dir_to_delete}: {clean_e}")

        print(f"Finished processing run {runId}. Final status: {run_status}")
        print("-" * 20)  # Separator for logs


# --- RabbitMQ Connection and Consumption ---
try:
    parameters = pika.URLParameters(RABBITMQ_URL)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    # Set QoS to prevent a worker from grabbing too many messages it can't handle
    channel.basic_qos(prefetch_count=1)

    print("Waiting for messages...")
    channel.basic_consume(
        queue=QUEUE_NAME, on_message_callback=process_message
    )  # Auto-ack is False by default

    channel.start_consuming()

except pika.exceptions.AMQPConnectionError as conn_err:
    print(f"Failed to connect to RabbitMQ: {conn_err}")
    exit(1)
except KeyboardInterrupt:
    print("Stopping worker...")
    if "connection" in locals() and connection.is_open:
        connection.close()
    print("Worker stopped.")
except Exception as e:
    print(f"An unexpected error occurred in the main loop: {e}")
    if "connection" in locals() and connection.is_open:
        connection.close()
    exit(1)
