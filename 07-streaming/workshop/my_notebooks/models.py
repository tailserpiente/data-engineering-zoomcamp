import json
import dataclasses

from dataclasses import dataclass


@dataclass
class Ride:
    lpep_pickup_datetime: int  # epoch milliseconds
    lpep_dropoff_datetime: int  # epoch milliseconds
    PULocationID: int
    DOLocationID: int
    passenger_count: int
    trip_distance: float
    tip_amount: float
    total_amount: float



def _safe_int(value, default=0):
    # Handles None/NaN values from pandas rows.
    if value is None:
        return default
    if isinstance(value, float) and value != value:
        return default
    return int(value)


def ride_from_row(row):
    return Ride(
        PULocationID=_safe_int(row['PULocationID']),
        DOLocationID=_safe_int(row['DOLocationID']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        lpep_pickup_datetime=int(row['lpep_pickup_datetime'].timestamp() * 1000),
        lpep_dropoff_datetime=int(row['lpep_dropoff_datetime'].timestamp() * 1000),
        passenger_count=_safe_int(row['passenger_count']),
        tip_amount=float(row['tip_amount']),
    )


def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    ride_json = json.dumps(ride_dict).encode('utf-8')
    return ride_json


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
