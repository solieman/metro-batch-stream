"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
import time
import json
from typing import Callable
from concurrent import futures
from google.cloud import pubsub_v1
from google.oauth2 import service_account

from random_trips import generate_message
from auth import auth_key

#

# TODO(developer): Set key_path to the path to the service account key
#                  file.
key_path = auth_key.get_key_path()
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

#


project_id = "first-project-186210"
topic_id = "mrt-demo"
# projects/first-project-186210/topics/mrt-demo

publisher = pubsub_v1.PublisherClient(credentials=credentials)
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []


def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback


for i in range(1000):
    time.sleep(1)
    data = generate_message()
    data_str = json.dumps(data)
    # When you publish a message, the client returns a future.
    publish_future = publisher.publish(topic_path, data_str.encode("utf-8"))
    # Non-blocking. Publish failures are handled in the callback function.
    publish_future.add_done_callback(get_callback(publish_future, data_str))
    publish_futures.append(publish_future)

# Wait for all the publish futures to resolve before exiting.
futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

print(f"Published messages with error handler to {topic_path}.")
