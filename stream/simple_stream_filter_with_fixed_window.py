import argparse
import logging
import json
from datetime import datetime

# Beam
import apache_beam as beam
from apache_beam import (
    DoFn,
    io,
)
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.transforms import window
from dateutil import parser
from google.oauth2 import service_account

from auth.auth_key import get_subscription_name, get_key_path


# Auth
import os

key_path = get_key_path()
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path


class DecodePubSubMessages(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each element by extracting the message body."""
        # print(publish_time)
        # print(datetime.utcfromtimestamp(float(publish_time)).strftime("%Y-%m-%d %H:%M:%S.%f"))
        yield element.decode("utf-8")


class DecodePubSubMessagesWithTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its publish time into a tuple."""
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )


class AddTimestampDoFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        # Extract the numeric Unix seconds-since-epoch timestamp to be
        # associated with the current log entry.
        # receive_timestamp = element.get("receiveTimestamp", None)
        receive_timestamp = element.get("from_timestamp", datetime.now())
        timestamp = parser.parse(receive_timestamp).timestamp()
        # Wrap and emit the current entry and new timestamp in a
        # TimestampedValue.
        yield window.TimestampedValue(element, timestamp)


def run_simple_stream(argv=None, save_main_session=True):
    pipeline_options = PipelineOptions(streaming=True, save_main_session=True)

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as pipeline:

        load_data = (
            pipeline
            | "Read from Pub/Sub"
            >> io.ReadFromPubSub(subscription=get_subscription_name())
            | "decode the messages" >> beam.ParDo(DecodePubSubMessages())
            | "Filter On Keyword" >> beam.Filter(lambda line: "Bukit Jalil" in line)
            | "from string to json" >> beam.ParDo(lambda x: [json.loads(x)])
            | "timestamp" >> beam.ParDo(AddTimestampDoFn())
            | "Assign Key" >> beam.Map(lambda line: ("Bukit Jalil", 1))
            | "Window" >> beam.WindowInto(window.FixedWindows(0.5 * 60))
            | "Count Appearance" >> beam.combiners.Count.PerKey()
            | "Print Count" >> beam.Map(lambda item: print(item))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_simple_stream()
