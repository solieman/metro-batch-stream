import json
import logging
from datetime import datetime
from dateutil import parser

# Beam
import apache_beam as beam
from apache_beam.transforms import window, trigger


class AddTimestampDoFn(beam.DoFn):
    def process(self, element, *args, **kwargs):
        """

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


def run_per_station_batch():

    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Read Batch Data" >> beam.io.ReadFromText("data.jsonl")
            | "Filter On Keyword" >> beam.Filter(lambda line: "Bukit Jalil" in line)
            | "from string to json" >> beam.ParDo(lambda x: [json.loads(x)])
            | "key object mapping" >> beam.Map(lambda x: (x["from_station"], x))
            | "Count Appearance" >> beam.combiners.Count.PerKey()
            | "Print Count" >> beam.Map(lambda item: print(item))
        )

    pass


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_per_station_batch()
