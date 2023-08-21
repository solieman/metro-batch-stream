import json
import logging

# Beam
import apache_beam as beam


def run_per_route_batch():

    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Read Batch Data" >> beam.io.ReadFromText("data.jsonl")
            | "from string to json" >> beam.ParDo(lambda x: [json.loads(x)])
            | "key object mapping"
            >> beam.Map(lambda x: ((x["from_station"], x["to_station"]), x))
            | "Count Appearance" >> beam.combiners.Count.PerKey()
            | "Print Count" >> beam.Map(lambda item: print(item))
        )

    pass


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_per_route_batch()
