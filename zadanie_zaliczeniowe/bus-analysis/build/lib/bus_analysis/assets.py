"""Assets module for bus analysis project."""

import datetime
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import folium
from dagster import asset, get_dagster_logger, MetadataValue, AssetExecutionContext
from sklearn.metrics.pairwise import haversine_distances
from .resources import WarsawApiResource
from .utils.geo_utils import haversine

log = get_dagster_logger()


@asset(io_manager_key="base_io_manager", group_name="bus")
def fetch_buses_data(warsaw_api: WarsawApiResource):
    """Fetches buses data over a period of time."""
    return warsaw_api.request_loc_in_time(15)


@asset(io_manager_key="base_io_manager", group_name="bus")
def fetch_stops_data(warsaw_api: WarsawApiResource):
    """Fetches data for all bus and tram stops."""
    result = warsaw_api.request_stops()
    log.info(result)
    return result


@asset(io_manager_key="base_io_manager", group_name="bus")
def fetch_routes_data(warsaw_api: WarsawApiResource):
    """Fetches data for all bus and tram routes."""
    result = warsaw_api.request_routes()
    log.info(result)
    return result


@asset(io_manager_key="base_io_manager", group_name="bus")
def fetch_timetables_data(warsaw_api: WarsawApiResource, fetch_routes_data):
    """Fetches timetables data for all routes."""
    timetables = []
    for _, row in fetch_routes_data.iterrows():
        busstop_id, busstop_nr, line = (
            str(row["nr_zespolu"]),
            str(row["nr_przystanku"]),
            str(row["route"]),
        )
        times = warsaw_api.request_timetables(busstop_id, busstop_nr, line)
        timetables.append(times)
        log.info(
            f"Times for route {line}, busstop_id {busstop_id}, busstop_nr {busstop_nr}: {times}"
        )
    fetch_routes_data["times"] = timetables
    return fetch_routes_data


@asset(io_manager_key="base_io_manager", group_name="bus")
def analyze_speed_violation_by_location(
    context: AssetExecutionContext,
    analyze_bus_speed: pd.DataFrame,
    buses_with_nearest_stops: pd.DataFrame,
    fetch_stops_data: pd.DataFrame,
):
    """Analyzes speed violation by location and creates a map of significant violations."""
    fetch_stops_data["szer_geo"] = pd.to_numeric(
        fetch_stops_data["szer_geo"], errors="coerce"
    )
    fetch_stops_data["dlug_geo"] = pd.to_numeric(
        fetch_stops_data["dlug_geo"], errors="coerce"
    )
    avg_coords_per_stop = (
        fetch_stops_data.groupby("zespol")[["szer_geo", "dlug_geo"]]
        .mean()
        .reset_index()
    )
    too_fast_buses_with_stops = pd.merge(
        analyze_bus_speed,
        buses_with_nearest_stops[["VehicleNumber", "Time", "nearest_stop"]],
        on=["VehicleNumber", "Time"],
        how="left",
    )
    violations_per_location = too_fast_buses_with_stops.groupby("nearest_stop").size()
    total_measurements = buses_with_nearest_stops.groupby("nearest_stop")[
        "VehicleNumber"
    ].nunique()
    violation_summary = pd.DataFrame(
        {
            "Total Violations": violations_per_location,
            "Total Measurements": total_measurements,
        }
    ).fillna(0)
    violation_summary["Percentage"] = (
        100
        * violation_summary["Total Violations"]
        / violation_summary["Total Measurements"]
    )
    significant_violations = violation_summary[
        violation_summary["Percentage"] >= 20
    ].reset_index()
    significant_violations = significant_violations.merge(
        avg_coords_per_stop, left_on="nearest_stop", right_on="zespol"
    )
    m = folium.Map(location=[52.2296756, 21.0122287], zoom_start=10)
    for _, row in significant_violations.iterrows():
        folium.Marker(
            location=[row["szer_geo"], row["dlug_geo"]],
            popup=f"""
            Stop ID: {row['nearest_stop']}
            \nViolations: {row['Total Violations']}
            \nMeasurements: {row['Total Measurements']}
            \nPercentage: {row['Percentage']}%",
            """,
            icon=folium.Icon(color="red", icon="info-sign"),
        ).add_to(m)
    m.save("../maps/significant_violations_map.html")
    context.add_output_metadata(
        {
            "Significant violations": MetadataValue.md(
                significant_violations.to_markdown()
            ),
            "All the violations": MetadataValue.md(violation_summary.to_markdown()),
        }
    )
    return significant_violations


@asset(io_manager_key="base_io_manager", group_name="bus")
def analyze_bus_speed(context: AssetExecutionContext, fetch_buses_data: pd.DataFrame):
    """Analyzes bus speeds, identifying buses moving too fast."""
    buses_data = fetch_buses_data.sort_values(by=["VehicleNumber", "Time"])
    speeds = [np.nan]
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
                ).total_seconds() / 3600
                speed = dist / time_diff if time_diff > 0 else 0
                speeds.append(speed)
            except Exception as e:
                log.info(f"Error calculating speed: {e}")
                speeds.append(np.nan)
        else:
            speeds.append(np.nan)
    buses_data["Speed"] = speeds
    too_fast_buses = buses_data[
        (buses_data["Speed"] > 50) & (buses_data["Speed"] < 100)
    ]
    help_df = buses_data[(buses_data["Speed"] > 3) & (buses_data["Speed"] < 100)]
    average_bus_speed = float(help_df["Speed"].mean())
    context.add_output_metadata(
        {
            "Too fast buses": MetadataValue.md(too_fast_buses.head().to_markdown()),
            "All the buses": MetadataValue.md(buses_data.head().to_markdown()),
            "Average bus speed (not standing)": average_bus_speed,
        }
    )
    return too_fast_buses


def find_nearest_stop(stops_df: pd.DataFrame, buses_data: pd.DataFrame) -> pd.DataFrame:
    """Finds the nearest stop for each bus."""
    stops_df["lat_rad"], stops_df["lon_rad"] = np.radians(
        pd.to_numeric(stops_df["szer_geo"], errors="coerce")
    ), np.radians(pd.to_numeric(stops_df["dlug_geo"], errors="coerce"))
    buses_data["lat_rad"], buses_data["lon_rad"] = np.radians(
        pd.to_numeric(buses_data["Lat"], errors="coerce")
    ), np.radians(pd.to_numeric(buses_data["Lon"], errors="coerce"))
    buses_data["nearest_stop"] = np.nan
    buses_data["nearest_stop_number"] = np.nan
    buses_data["distance_to_stop"] = np.nan
    buses_data["is_at_stop"] = False
    for line, group in stops_df.groupby("route"):
        line_buses = buses_data[
            (buses_data["Lines"] == line)
            & buses_data["lat_rad"].notna()
            & buses_data["lon_rad"].notna()
        ]
        if (
            line_buses.empty
            or group.empty
            or group["lat_rad"].isna().all()
            or group["lon_rad"].isna().all()
        ):
            continue
        bus_coords = line_buses[["lat_rad", "lon_rad"]].to_numpy()
        stop_coords = group[["lat_rad", "lon_rad"]].dropna().to_numpy()
        distances = (
            haversine_distances(bus_coords, stop_coords) * 6371000
        )  # Earth radius in meters
        for i, bus_idx in enumerate(line_buses.index):
            nearest_stop_idx = distances[i].argmin()
            buses_data.at[bus_idx, "nearest_stop"] = group.iloc[nearest_stop_idx][
                "nr_zespolu"
            ]
            buses_data.at[bus_idx, "nearest_stop_number"] = group.iloc[
                nearest_stop_idx
            ]["nr_przystanku"]
            buses_data.at[bus_idx, "distance_to_stop"] = distances[i, nearest_stop_idx]
            buses_data.at[bus_idx, "is_at_stop"] = (
                distances[i, nearest_stop_idx] <= 15
            )  # 15 meters threshold for being at a stop
    return buses_data


@asset(io_manager_key="base_io_manager", group_name="bus")
def buses_with_nearest_stops(
    context: AssetExecutionContext,
    fetch_buses_data,
    fetch_stops_data,
    fetch_timetables_data,
):
    """Combines bus data with nearest stops and timetables for punctuality analysis."""
    routes_df = pd.merge(
        fetch_timetables_data,
        fetch_stops_data,
        how="left",
        left_on=["nr_zespolu", "nr_przystanku"],
        right_on=["zespol", "slupek"],
    )
    buses_df = find_nearest_stop(routes_df, fetch_buses_data)
    context.add_output_metadata(
        {"Nearest stops": MetadataValue.md(buses_df.head().to_markdown())}
    )
    return buses_df


def process_time(time_str, base_date):
    """Converts time string to datetime object, adjusting for hours >24."""
    hours, minutes, seconds = map(int, time_str.split(":"))
    additional_days, corrected_hours = divmod(hours, 24)
    corrected_datetime = datetime(
        base_date.year,
        base_date.month,
        base_date.day,
        corrected_hours,
        minutes,
        seconds,
    )
    corrected_datetime += timedelta(days=additional_days)
    return corrected_datetime


def find_punctuality(route_df: pd.DataFrame, buses_df: pd.DataFrame) -> pd.DataFrame:
    """Calculates lateness for buses at stops based on timetables."""
    buses_df["lateness"] = np.nan
    for index, bus in buses_df.iterrows():
        if bus["is_at_stop"]:
            base_date = datetime.strptime(bus["Time"].split(" ")[0], "%Y-%m-%d")
            scheduled_times_df = route_df.loc[
                (route_df["nr_zespolu"] == bus["nearest_stop"])
                & (route_df["nr_przystanku"] == bus["nearest_stop_number"])
                & (route_df["route"] == bus["Lines"]),
                ["times", "bus_id"],
            ]
            if scheduled_times_df.empty:
                continue
            scheduled_times = scheduled_times_df.iloc[0]["times"]
            is_first_stop = scheduled_times_df.iloc[0]["bus_id"] == 1
            if is_first_stop:
                lateness = 0  # Zero lateness at first stop
            else:
                if len(scheduled_times) == 0:
                    continue
                actual_time = datetime.strptime(bus["Time"], "%Y-%m-%d %H:%M:%S")
                processed_times = [
                    process_time(time, base_date) for time in scheduled_times
                ]
                preceding_times = [
                    time
                    for time in processed_times
                    if time <= actual_time + timedelta(minutes=2)
                ]
                closest_preceding_time = (
                    max(preceding_times)
                    if preceding_times
                    else min(
                        processed_times,
                        key=lambda x: abs((x - actual_time).total_seconds()),
                    )
                )
                lateness = (actual_time - closest_preceding_time).total_seconds() / 60
            buses_df.at[index, "lateness"] = lateness
    buses_df.to_csv("../data/punctuality.csv")
    return buses_df


@asset(io_manager_key="base_io_manager", group_name="bus")
def analyze_bus_punctuality(
    context: AssetExecutionContext,
    buses_with_nearest_stops: pd.DataFrame,
    fetch_stops_data: pd.DataFrame,
    fetch_timetables_data: pd.DataFrame,
):
    """Analyzes bus punctuality based on nearest stop and timetable data."""
    routes_df = pd.merge(
        fetch_timetables_data,
        fetch_stops_data,
        how="left",
        left_on=["nr_zespolu", "nr_przystanku"],
        right_on=["zespol", "slupek"],
    )
    buses_df = find_punctuality(routes_df, buses_with_nearest_stops)
    context.add_output_metadata(
        {
            "Punctuality": MetadataValue.md(buses_df.head().to_markdown()),
            "Average lateness": float(buses_df["lateness"].mean()),
        }
    )
    return buses_df
