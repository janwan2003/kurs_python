import datetime
from dagster import asset, get_dagster_logger
from .resources import WarsawApiResource
from .utils.geo_utils import haversine
from geopy.distance import geodesic
import pandas as pd
import numpy as np

log = get_dagster_logger()


@asset(
    io_manager_key="base_io_manager",
    group_name="bus",
)
def fetch_buses_data(warsaw_api: WarsawApiResource):
    return warsaw_api.request_loc_in_time(15)


@asset(
    io_manager_key="base_io_manager",
    group_name="bus",
)
def fetch_stops_data(warsaw_api: WarsawApiResource):
    result = warsaw_api.request_stops()
    log.info(result)
    return result


@asset(
    io_manager_key="base_io_manager",
    group_name="bus",
)
def fetch_routes_data(warsaw_api: WarsawApiResource):
    result = warsaw_api.request_routes()
    log.info(result)
    return result


@asset(
    io_manager_key="base_io_manager",
    group_name="bus",
)
def fetch_timetables_data(warsaw_api: WarsawApiResource, fetch_routes_data):
    timetables = []
    for index, row in fetch_routes_data.iterrows():
        busstopId = str(row["nr_zespolu"])
        busstopNr = str(row["nr_przystanku"])
        line = str(row["route"])
        times = warsaw_api.request_timetables(busstopId, busstopNr, line)
        timetables.append(times)
        log.info(
            f"Times for route {line}, busstopId {busstopId}, busstopNr {busstopNr}: {times}"
        )
    fetch_routes_data["times"] = timetables

    return fetch_routes_data


def analyze_speed_violation_by_location(
    too_fast_buses: pd.DataFrame, buses_with_nearest_stops: pd.DataFrame
):
    # Merge too_fast_buses with buses_with_nearest_stops to include nearest_stop information
    merged_data = pd.merge(
        too_fast_buses,
        buses_with_nearest_stops[["VehicleNumber", "Time", "nearest_stop"]],
        on=["VehicleNumber", "Time"],
        how="left",
    )

    # Ensure there's a 'nearest_stop' column in merged_data for this analysis
    if "nearest_stop" not in merged_data.columns:
        log.error("Nearest stop data is missing from the merged buses data.")
        return pd.DataFrame()

    # Perform analysis on merged_data now that it includes nearest_stop
    violations_per_location = merged_data.groupby("nearest_stop").size()
    total_measurements_per_location = merged_data.groupby("nearest_stop")[
        "VehicleNumber"
    ].nunique()
    violation_percentages = (
        violations_per_location / total_measurements_per_location
    ) * 100

    violation_summary = pd.DataFrame(
        {
            "Total Violations": violations_per_location,
            "Total Measurements": total_measurements_per_location,
            "Violation Percentage": violation_percentages,
        }
    )

    significant_violations = violation_summary[
        violation_summary["Violation Percentage"] > 10
    ]

    log.info(f"Locations with significant speed violations: {significant_violations}")
    significant_violations.to_csv("../data/significant_speed_violations.csv")

    return significant_violations


@asset(io_manager_key="base_io_manager", group_name="bus")
def analyze_bus_speed(fetch_buses_data):

    buses_data = fetch_buses_data.sort_values(
        by=["VehicleNumber", "Time"]
    )  # Sortowanie danych

    # Obliczanie prędkości
    speeds = []
    for i in range(1, len(buses_data)):
        if (
            buses_data.iloc[i]["VehicleNumber"]
            == buses_data.iloc[i - 1]["VehicleNumber"]
        ):
            try:
                dist = haversine(
                    buses_data.iloc[i - 1]["Lon"],
                    buses_data.iloc[i - 1]["Lat"],
                    buses_data.iloc[i]["Lon"],
                    buses_data.iloc[i]["Lat"],
                )
                time_diff = (
                    pd.to_datetime(buses_data.iloc[i]["Time"])
                    - pd.to_datetime(buses_data.iloc[i - 1]["Time"])
                ).seconds / 3600
                speed = dist / time_diff if time_diff > 0 else 0
                speeds.append(speed)
            except Exception as e:
                log.info(e)
                speeds.append(np.nan)
        else:
            speeds.append(np.nan)  # NaN dla pierwszego pomiaru każdego autobusu

    buses_data["Speed"] = [
        np.nan
    ] + speeds  # Pierwszy rekord nie ma poprzednika, więc prędkość jest nieznana

    too_fast_buses = buses_data[buses_data["Speed"] > 50 and buses_data["Speed"] < 100]
    # Autobusy poruszające się ponad 100 na godzinę to anomalia

    log.info(too_fast_buses)
    too_fast_buses.to_csv("../data/tofast.csv")

    return too_fast_buses


def find_nearest_stop(stops_df: pd.DataFrame, buses_data: pd.DataFrame) -> pd.DataFrame:
    nearest_stops = []
    distances_to_nearest = []
    nearest_stop_numbers = []

    for index, bus in buses_data.iterrows():
        line_stops = stops_df[stops_df["route"] == bus["Lines"]]
        try:
            if pd.isna(bus["Lat"]) or pd.isna(bus["Lon"]) or line_stops.empty:
                nearest_stops.append(np.nan)
                distances_to_nearest.append(np.nan)
                continue
            distances = line_stops.apply(
                lambda stop: (
                    geodesic(
                        (stop["szer_geo"], stop["dlug_geo"]), (bus["Lat"], bus["Lon"])
                    ).meters
                    if not (pd.isna(stop["szer_geo"]) or pd.isna(stop["dlug_geo"]))
                    else np.nan
                ),
                axis=1,
            )
            nearest_idx = distances.idxmin()

            nearest_stops.append(line_stops.loc[nearest_idx, "nr_zespolu"])
            distances_to_nearest.append(distances.min())
            nearest_stop_numbers.append(line_stops.loc[nearest_idx, "nr_przystanku"])
        except Exception as e:
            log.info(e)
            nearest_stops.append(np.nan)
            distances_to_nearest.append(np.nan)

    buses_data["nearest_stop"] = nearest_stops
    buses_data["nearest_stop_number"] = nearest_stop_numbers
    buses_data["distance_to_stop"] = distances_to_nearest
    buses_data["is_at_stop"] = np.where(
        buses_data["distance_to_stop"] <= 15, True, False
    )

    return buses_data


def find_punctuality(route_df: pd.DataFrame, buses_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates how late or early each bus is compared to the timetable.
    """
    for index, row in route_df.iterrows():
        route_df.at[index, "times"] = [
            datetime.strptime(time, "%H:%M:%S") for time in row["times"]
        ]

    buses_df["lateness"] = np.nan

    for index, bus in buses_df.iterrows():
        if bus["is_at_stop"]:
            scheduled_times = route_df.loc[
                (route_df["nr_zespolu"] == bus["nearest_stop"])
                & (route_df["nr_przystanku"] == bus["nearest_stop_number"])
                & (route_df["route"] == bus["Lines"]),
                "times",
            ].values[0]

            # Convert bus['Time'] to a datetime object for comparison
            actual_time = datetime.strptime(bus["Time"], "%Y-%m-%d %H:%M:%S")

            # Filter scheduled times to only include those before or equal to the actual time
            preceding_times = [
                time for time in scheduled_times if time <= actual_time + 2
            ]

            if preceding_times:
                closest_preceding_time = max(preceding_times)
                lateness = (actual_time - closest_preceding_time).total_seconds() / 60
            else:
                closest_preceding_time = min(scheduled_times)
                lateness = -(
                    (closest_preceding_time - actual_time).total_seconds() / 60
                )

            buses_df.at[index, "lateness"] = lateness

    return buses_df


@asset(io_manager_key="base_io_manager", group_name="bus")
def buses_with_nearest_stops(fetch_buses_data, fetch_stops_data, fetch_timetables_data):
    log.info(fetch_timetables_data)
    fetch_timetables_data.to_csv("../data/timetables.csv")
    fetch_stops_data.to_csv("../data/just_stops.csv")
    fetch_buses_data.to_csv("../data/buses_online.csv")
    # fetch_timetables_data zawiera dane o wszystkich odjazdach dla każdego przystanku z trasy
    routes_df = pd.merge(
        fetch_timetables_data,
        fetch_stops_data,
        how="left",
        left_on=["nr_zespolu", "nr_przystanku"],
        right_on=["zespol", "slupek"],
    )
    log.info(routes_df)
    routes_df.to_csv("../data/with_stops.csv")
    # Zapisuję csv i loguję tylko dla pomocy, aby ułatwić przyjżenie się danym
    buses_df = find_nearest_stop(routes_df, fetch_buses_data)
    buses_df.to_csv("../data/buses_with_stops.csv")
    return buses_df


@asset(io_manager_key="base_io_manager", group_name="bus")
def analyze_bus_punctuality(
    buses_with_nearest_stops: pd.DataFrame,
):
    buses_df = find_punctuality(buses_with_nearest_stops, buses_df)
    on_stops = buses_df[buses_df["is_at_stop"]]
    on_stops.to_csv("../data/punctuality.csv")
    return buses_df
