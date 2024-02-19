"""Module for accessing Warsaw public transport API."""

import time
import requests
import pandas as pd
from dagster import ConfigurableResource, get_dagster_logger

API_URL = "https://api.um.warszawa.pl/api/action/"
TYPE = "1"  # 1 for buses, 2 for trams


def flatten_data_routes(data):
    """Flatten nested route data into a list of dictionaries for easier processing."""
    flattened = []
    for route, route_data in data.items():
        for direction, direction_data in route_data.items():
            for bus_id, bus_info in direction_data.items():
                bus_info.update(
                    {"route": route, "direction": direction, "bus_id": bus_id}
                )
                flattened.append(bus_info)
    return flattened


class WarsawApiResource(ConfigurableResource):
    """A configurable resource for accessing the Warsaw public transport API."""

    api_key: str

    def request_loc(self):
        """Requests current location data for buses or trams."""
        params = {
            "resource_id": "f2e5503e-927d-4ad3-9500-4ab9e55deb59",
            "apikey": self.api_key,
            "type": TYPE,
        }
        response = requests.get(API_URL + "busestrams_get", params=params, timeout=10)
        if response.status_code == 200:
            get_dagster_logger().info("Data fetched: " + str(response.json()["result"]))
            return pd.DataFrame(response.json()["result"])
        get_dagster_logger().error(f"Data fetch error: {response.status_code}")
        raise ValueError(f"Data fetch error: {response.status_code}")

    def request_loc_in_time(self, minutes):
        """Requests location data for buses or trams over a specified period."""
        buses = pd.DataFrame()
        for _ in range(2 * minutes):
            data = pd.DataFrame()
            while data.empty:
                try:
                    data = self.request_loc()
                except Exception as e:
                    get_dagster_logger().info(str(e))
                    time.sleep(30)
            buses = pd.concat([buses, data], ignore_index=True)
        buses.drop_duplicates(inplace=True)
        return buses

    def request_stops(self):
        """Requests data for all bus and tram stops."""
        params = {
            "id": "ab75c33d-3a26-4342-b36a-6e5fef0a3ac3",
            "apikey": self.api_key,
        }
        response = requests.get(API_URL + "dbstore_get", params=params, timeout=10)
        if response.status_code == 200:
            get_dagster_logger().info("Data fetched: " + str(response.json()["result"]))
            return pd.DataFrame(
                [entry["values"] for entry in response.json()["result"]]
            )
        get_dagster_logger().error(f"Data fetch error: {response.status_code}")
        raise ValueError(f"Data fetch error: {response.status_code}")

    def request_timetables(self, busstop_id, busstop_nr, line):
        """Requests timetable data for a specific bus or tram stop."""
        params = {
            "id": "e923fa0e-d96c-43f9-ae6e-60518c9f3238",
            "apikey": self.api_key,
            "busstopId": busstop_id,
            "busstopNr": busstop_nr,
            "line": line,
        }
        response = requests.get(API_URL + "dbtimetable_get", params=params, timeout=10)
        if response.status_code == 200:
            get_dagster_logger().info("Data fetched: " + str(response.json()["result"]))
            return [
                item["value"]
                for entry in response.json()["result"]
                for item in entry["values"]
                if item["key"] == "czas"
            ]

        get_dagster_logger().error(f"Data fetch error: {response.status_code}")
        raise ValueError(f"Data fetch error: {response.status_code}")

    def request_routes(self):
        """Requests data for all bus and tram routes."""
        params = {
            "apikey": self.api_key,
        }
        response = requests.get(
            API_URL + "public_transport_routes", params=params, timeout=10
        )
        if response.status_code == 200:
            get_dagster_logger().info("Data fetched: " + str(response.json()["result"]))
            return pd.DataFrame(flatten_data_routes(response.json()["result"]))

        get_dagster_logger().error(f"Data fetch error: {response.status_code}")
        raise ValueError(f"Data fetch error: {response.status_code}")
