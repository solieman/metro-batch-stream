import logging

# Beam
import apache_beam as beam


def run_simple_batch_01():
    """
    Executing simple batch job by Apache Beam
    Read data from data.jsonl file
    filter records with the Bukit Jalil station only
    count the valid records globally
    print the total count
    :return: None
    """

    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Read Batch Data" >> beam.io.ReadFromText("data.jsonl")
            | "Filter On Keyword" >> beam.Filter(lambda line: "Bukit Jalil" in line)
            | "Count Appearance" >> beam.combiners.Count.Globally()
            | "Print Count" >> beam.Map(lambda item: print(item))
        )

    pass


def run_simple_batch_02():
    """
    Executing simple batch job by Apache Beam
    Read data from data.jsonl file
    filter records with the Bukit Jalil station only
    map 'Bukit Jalil' as a key and 1 as a value for each valid record
    count records based on key
    print the total count

    :return:
    """

    with beam.Pipeline() as pipeline:
        (
            pipeline
            | "Read Batch Data" >> beam.io.ReadFromText("data.jsonl")
            | "Filter On Keyword" >> beam.Filter(lambda line: "Bukit Jalil" in line)
            | "Assign Key" >> beam.Map(lambda line: ("Bukit Jalil", 1))
            | "Count Appearance" >> beam.combiners.Count.PerKey()
            | "Print Count" >> beam.Map(lambda item: print(item))
        )

    pass


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run_simple_batch_02()
