from importlib.resources import files
import csv
from typing import Iterator

def init_data():
    csv_path = files("kafka").joinpath("data/flights.csv")
    with csv_path.open() as f:
        reader: Iterator[list[str]] = csv.reader(f, delimiter=",")
        for row in reader:
            print(row) 

def upload_from_database_to_kafka():
    pass


def sink_from_kafka_to_database():
    pass