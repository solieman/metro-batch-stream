import random
import json
import math
import uuid
from datetime import datetime, timedelta

stations = ["Bukit Jalil","Masjid Jamek","Bandar Tasik Selatan","Sungai Besi","Pandan Jaya","Cheras","Batang Benar","KL Sentral","Salak Selatan","Kajang","Putrjaya","Serdang","Rawang","Wawasan","Batu Tiga"]


def add_to_json(item: dict):
    """

    :param item:
    :return:
    """
    # Serializing json
    json_object = json.dumps(item)

    # Writing to jsonlines file
    with open("data.jsonl", "a") as jsonl_output:
        # Write the data into the file as one line
        jsonl_output.write(json_object)
        # Add new line
        jsonl_output.write("\n")
        jsonl_output.close()

def generate_message():
    # datetime object containing current date and time
    now = datetime.now().utcnow()
    dt_string = now.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    arrival_time = now + timedelta(minutes=1)
    at_string = arrival_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    random_uuid = str(uuid.uuid4())[:28].replace("-", "")

    final_obj = {
        "passenger_id": random_uuid,
        "from_station": random.choice(stations),
        "to_station": random.choice(stations),
        "from_timestamp": dt_string,
        "to_timestamp": at_string,
    }
    print(final_obj)
    add_to_json(final_obj)

    return final_obj