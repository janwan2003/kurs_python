import datetime
from dagster import asset, get_dagster_logger
from .resources import WarsawApiResource
from .utils.geo_utils import haversine
from sklearn.metrics.pairwise import haversine_distances
from geopy.distance import geodesic
from datetime import datetime, timedelta
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

@asset(io_manager_key="base_io_manager", group_name="bus")
def analyze_speed_violation_by_location(
    analyze_bus_speed: pd.DataFrame, buses_with_nearest_stops: pd.DataFrame
):
    total_measurements = buses_with_nearest_stops.groupby('nearest_stop')['VehicleNumber'].count() 
    too_fast_buses_with_stops = pd.merge(
        analyze_bus_speed,
        buses_with_nearest_stops[['VehicleNumber', 'Time', 'nearest_stop']],
        on=['VehicleNumber', 'Time'],
        how='left'
    )
    violations_per_location = too_fast_buses_with_stops.groupby('nearest_stop').size()
    
    log.info(f"total: {total_measurements}")
    log.info(f"violations: {violations_per_location}")
    violation_summary = pd.DataFrame({
        'Total Violations': violations_per_location,
        'Total Measurements': total_measurements
    }).fillna(0) 
    violation_summary["Percantage"] = 100 * violation_summary["Total Violations"] / violation_summary["Total Measurements"]
    significant_violations = violation_summary[violation_summary["Percantage"] >= 20]
    log.info(significant_violations)

    return significant_violations


@asset(io_manager_key="base_io_manager", group_name="bus")
def analyze_bus_speed(fetch_buses_data: pd.DataFrame) -> pd.DataFrame:
    buses_data = fetch_buses_data.sort_values(by=["VehicleNumber", "Time"])

    # Assuming the haversine function is correctly defined elsewhere
    speeds = [np.nan]  # Initialize with NaN for the first record
    for i in range(1, len(buses_data)):
        if buses_data.iloc[i]["VehicleNumber"] == buses_data.iloc[i - 1]["VehicleNumber"]:
            try:
                dist = haversine(
                    buses_data.iloc[i - 1]["Lon"],
                    buses_data.iloc[i - 1]["Lat"],
                    buses_data.iloc[i]["Lon"],
                    buses_data.iloc[i]["Lat"],
                )
                time_diff = (pd.to_datetime(buses_data.iloc[i]["Time"]) - pd.to_datetime(buses_data.iloc[i - 1]["Time"])).total_seconds() / 3600
                speed = dist / time_diff if time_diff > 0 else 0
                speeds.append(speed)
            except Exception as e:
                log.info(f"Error calculating speed: {e}")
                speeds.append(np.nan)
        else:
            speeds.append(np.nan)  # Append NaN for the first measurement of each bus

    buses_data["Speed"] = speeds

    # Correct filtering for too_fast_buses using bitwise '&' operator with parentheses around conditions
    too_fast_buses = buses_data[(buses_data["Speed"] > 50) & (buses_data["Speed"] < 100)]

    log.info(too_fast_buses)
    too_fast_buses.to_csv("../data/too_fast.csv")

    return too_fast_buses



def find_nearest_stop(stops_df: pd.DataFrame, buses_data: pd.DataFrame) -> pd.DataFrame:
    stops_df["lat_rad"], stops_df["lon_rad"] = np.radians(pd.to_numeric(stops_df["szer_geo"], errors='coerce')), np.radians(pd.to_numeric(stops_df["dlug_geo"], errors='coerce'))
    buses_data["lat_rad"], buses_data["lon_rad"] = np.radians(pd.to_numeric(buses_data["Lat"], errors='coerce')), np.radians(pd.to_numeric(buses_data["Lon"], errors='coerce'))
    
    buses_data["nearest_stop"] = np.nan
    buses_data["nearest_stop_number"] = np.nan
    buses_data["distance_to_stop"] = np.nan
    buses_data["is_at_stop"] = False

    for line, group in stops_df.groupby("route"):
        line_buses = buses_data[(buses_data["Lines"] == line) & buses_data["lat_rad"].notna() & buses_data["lon_rad"].notna()]
        
        if line_buses.empty or group.empty or group["lat_rad"].isna().all() or group["lon_rad"].isna().all():
            continue

        bus_coords = line_buses[["lat_rad", "lon_rad"]].to_numpy()
        stop_coords = group[["lat_rad", "lon_rad"]].dropna().to_numpy()
        
        distances = haversine_distances(bus_coords, stop_coords) * 6371000

        # Assign nearest stop information to buses
        for i, bus_idx in enumerate(line_buses.index):
            nearest_stop_idx = distances[i].argmin()
            buses_data.at[bus_idx, "nearest_stop"] = group.iloc[nearest_stop_idx]["nr_zespolu"]
            buses_data.at[bus_idx, "nearest_stop_number"] = group.iloc[nearest_stop_idx]["nr_przystanku"]
            buses_data.at[bus_idx, "distance_to_stop"] = distances[i, nearest_stop_idx]
            buses_data.at[bus_idx, "is_at_stop"] = distances[i, nearest_stop_idx] <= 15  # 15 meters

    return buses_data





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

def preprocess_time(time_str):
        # Split the time string into components
    hours, minutes, seconds = map(int, time_str.split(':'))
    
    # Calculate the overflow of hours beyond 24, and the additional days
    additional_days, corrected_hours = divmod(hours, 24)
    
    # Construct a new time string with corrected hours
    corrected_time_str = f"{corrected_hours:02d}:{minutes:02d}:{seconds:02d}"
    
    # Parse the corrected time string
    corrected_time = datetime.strptime(corrected_time_str, "%H:%M:%S")
    
    # Add the additional days to the datetime object
    corrected_time += timedelta(days=additional_days)
    
    return corrected_time

def find_punctuality(route_df: pd.DataFrame, buses_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates how late or early each bus is compared to the timetable.
    """
    for index, row in route_df.iterrows():
        route_df.at[index, "times"] = [preprocess_time(time) for time in row["times"]]

    
    buses_df["lateness"] = np.nan  # Initialize the lateness column

    for index, bus in buses_df.iterrows():
        if bus["is_at_stop"]:
            scheduled_times_df = route_df.loc[
                (route_df["nr_zespolu"] == bus["nearest_stop"]) &
                (route_df["nr_przystanku"] == bus["nearest_stop_number"]) &
                (route_df["route"] == bus["Lines"]),
                "times"
            ]

            if scheduled_times_df.empty:
                continue  # Skip if no scheduled times are found

            scheduled_times = scheduled_times_df.values[0]
            actual_time = datetime.strptime(bus["Time"], "%Y-%m-%d %H:%M:%S")

            # Adjusted logic to compare times correctly
            preceding_times = [time for time in scheduled_times if time <= (actual_time + datetime.timedelta(minutes=2))]

            if preceding_times:
                closest_preceding_time = max(preceding_times)
                lateness = (actual_time - closest_preceding_time).total_seconds() / 60
            else:
                closest_preceding_time = min(scheduled_times, key=lambda x: abs(x - actual_time))
                lateness = (actual_time - closest_preceding_time).total_seconds() / 60

            buses_df.at[index, "lateness"] = lateness

    return buses_df

@asset(io_manager_key="base_io_manager", group_name="bus")
def analyze_bus_punctuality(
    buses_with_nearest_stops: pd.DataFrame, fetch_stops_data: pd.DataFrame, fetch_timetables_data: pd.DataFrame
):
    routes_df = pd.merge(
        fetch_timetables_data,
        fetch_stops_data,
        how="left",
        left_on=["nr_zespolu", "nr_przystanku"],
        right_on=["zespol", "slupek"],
    )
    buses_df = find_punctuality(routes_df, buses_with_nearest_stops)
    on_stops = buses_df[buses_df["is_at_stop"]]
    on_stops.to_csv("../data/punctuality.csv")
    return buses_df
