import requests
import time
from dagster import ConfigurableResource, get_dagster_logger
import pandas as pd

API_URL = "https://api.um.warszawa.pl/api/action/"
TYPE = "1"  # 1 dla autobusów, 2 dla tramwajów


def flatten_data_routes(data):
    flattened = []
    for route, route_data in data.items():
        for direction, direction_data in route_data.items():
            for bus_id, bus_info in direction_data.items():
                # Add route and direction to bus_info dictionary
                bus_info["route"] = route
                bus_info["direction"] = direction
                bus_info["bus_id"] = bus_id
                flattened.append(bus_info)
    return flattened


class WarsawApiResource(ConfigurableResource):
    api_key: str

    def request_loc(self):
        params = {
            "resource_id": "f2e5503e-927d-4ad3-9500-4ab9e55deb59",
            "apikey": self.api_key,
            "type": TYPE,
        }
        response = requests.get(API_URL + "busestrams_get", params=params)
        if response.status_code == 200:
            get_dagster_logger().info("Pobrano dane: " + str(response.json()["result"]))
            return pd.DataFrame(response.json()["result"])
        else:
            get_dagster_logger().error(
                f"Błąd pobierania danych: {response.status_code}"
            )
            raise Exception(f"Błąd pobierania danych: {response.status_code}")

    def request_loc_in_time(self, minutes):
        buses = pd.DataFrame()
        for i in range(2 * minutes):
            data = pd.DataFrame()
            while data.empty:
                try:
                    data = self.request_loc()
                except Exception as e:
                    get_dagster_logger().info(e)
                    data = pd.DataFrame
            buses = pd.concat([buses, data], ignore_index=True)
            if i != (2 * minutes) - 1:
                time.sleep(30)
        buses.drop_duplicates(inplace=True)
        get_dagster_logger().info("Dane po usunięciu duplikatów:")
        get_dagster_logger().info(buses.head())
        return buses

    def request_stops(self):
        params = {
            "id": "ab75c33d-3a26-4342-b36a-6e5fef0a3ac3",
            "apikey": self.api_key,
        }
        response = requests.get(API_URL + "dbstore_get", params=params)
        if response.status_code == 200:
            get_dagster_logger().info("Pobrano dane: " + str(response.json()["result"]))
            processed_records = []
            for item in response.json()[
                "result"
            ]:  # Poprawka: iteracja po odpowiednim kluczu
                record = {}
                for entry in item["values"]:
                    record[entry["key"]] = entry["value"]
                processed_records.append(record)
            return pd.DataFrame(processed_records)
        else:
            get_dagster_logger().error(
                f"Błąd pobierania danych: {response.status_code}"
            )
            raise Exception(f"Błąd pobierania danych: {response.status_code}")

    def request_timetables(self, busstopId, busstopNr, line):
        params = {
            "id": "e923fa0e-d96c-43f9-ae6e-60518c9f3238",
            "apikey": self.api_key,
            "busstopId": busstopId,
            "busstopNr": busstopNr,
            "line": line,
        }
        response = requests.get(API_URL + "dbtimetable_get", params=params)
        if response.status_code == 200:
            get_dagster_logger().info("Pobrano dane: " + str(response.json()["result"]))
            times = [
                item["value"]
                for entry in response.json()["result"]
                for item in entry["values"]
                if item["key"] == "czas"
            ]
            return times
        else:
            get_dagster_logger().error(
                f"Błąd pobierania danych: {response.status_code}"
            )
            raise Exception(f"Błąd pobierania danych: {response.status_code}")

    def request_routes(self):
        params = {
            # "resource_id": "b61f1869-6f0a-4375-acdf-87ccffeecdf0",
            "apikey": self.api_key,
        }
        response = requests.get(API_URL + "public_transport_routes", params=params)
        if response.status_code == 200:
            get_dagster_logger().info("Pobrano dane: " + str(response.json()["result"]))
            flattened_data = flatten_data_routes(response.json()["result"])
            return pd.DataFrame(flattened_data)
        else:
            get_dagster_logger().error(
                f"Błąd pobierania danych: {response.status_code}"
            )
            raise Exception(f"Błąd pobierania danych: {response.status_code}")
