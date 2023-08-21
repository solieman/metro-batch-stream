import logging
import json
from datetime import datetime
from dateutil import parser

# Beam
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from apache_beam import DoFn, io, PTransform, PCollection, combiners
from apache_beam.transforms import window, trigger, combiners


class AddTimestampDoFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        """
        This function attach the event time to the PCollection

        :param element:
        :param args:
        :param kwargs:
        :return:
        """
        # Extract the numeric Unix seconds-since-epoch timestamp to be
        # associated with the current log entry.
        receive_timestamp = element.get("from_timestamp", datetime.now())
        timestamp = parser.parse(receive_timestamp).timestamp()
        # Wrap and emit the current entry and new timestamp in a
        # TimestampedValue.
        yield window.TimestampedValue(element, timestamp)


def run_window_batch():
    """

    :return: None
    """
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Read Batch Data" >> beam.io.ReadFromText("data.jsonl")
            | "Filter On Keyword" >> beam.Filter(lambda line: "Bukit Jalil" in line)
            | "from string to json" >> beam.ParDo(lambda x: [json.loads(x)])
            | "timestamp" >> beam.ParDo(AddTimestampDoFn())
            | "Assign Key" >> beam.Map(lambda x: (x["from_station"], x))
            | "window"
            >> beam.WindowInto(
                window.FixedWindows(1 * 60),
                trigger=trigger.DefaultTrigger(),
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
            )
            | "Count Appearance" >> beam.combiners.Count.PerKey()
            | "Print Count" >> beam.Map(lambda item: print(item))
        )

    pass


def run_window_batch_routes():
    """

    :return: None
    """
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Read Batch Data" >> beam.io.ReadFromText("data.jsonl")
            | "from string to json" >> beam.ParDo(lambda x: [json.loads(x)])
            | "timestamp" >> beam.ParDo(AddTimestampDoFn())
            | "key object mapping"
            >> beam.Map(lambda x: ((x["from_station"], x["to_station"]), x))
            | "Window"
            >> beam.WindowInto(
                window.FixedWindows(1 * 60),
                trigger=trigger.AfterWatermark(),
                accumulation_mode=trigger.AccumulationMode.DISCARDING,
            )
            | "Count Appearance" >> beam.combiners.Count.PerKey()
            | "Print Count" >> beam.Map(lambda item: print(item))
        )

    pass


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_window_batch()
