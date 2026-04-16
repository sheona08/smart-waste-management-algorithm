import math
import time
import subprocess
import json
from pathlib import Path
from typing import List, Dict, Tuple, Optional

import pandas as pd
import requests
from ortools.constraint_solver import pywrapcp, routing_enums_pb2


PREDICTION_SCRIPT = "predict_fill_levels.py"
PREDICTION_OUTPUT = "MeridianData_with_predictions.csv"
LIVE_READINGS_FILE = "latest_bin_readings.json"

MODE = "BALANCED_ROUTES"
FILL_THRESHOLD = 80.0
REMOTE_BIN_DISTANCE_KM = 10.0
VEHICLE_CAPACITY_L = 8000
MAX_ROUTE_MIN = 8 * 60
VEHICLE_SPEED_KMH = 30
DEFAULT_SERVICE_TIME_MIN = 5
OSRM_BASE_URL = "http://router.project-osrm.org"
OSRM_TIMEOUT = 20
OSRM_RETRIES = 3
LIVE_MAX_AGE_MIN = 60

DEPOT_NAME = "DEPOT_MAIN"
DEPOT_LAT = 37.143069
DEPOT_LON = -80.419532

WEEKDAY_TRUCKS = 7
WEEKEND_TRUCKS = 2

DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def safe_float(val, default=0.0):
    try:
        if pd.isna(val):
            return default
        return float(val)
    except Exception:
        return default


def safe_int(val, default=0):
    try:
        if pd.isna(val):
            return default
        return int(val)
    except Exception:
        return default


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def km_to_minutes(km: float, speed_kmh: float = VEHICLE_SPEED_KMH) -> int:
    if speed_kmh <= 0:
        return 0
    return int(round((km / speed_kmh) * 60))


def is_weekend(day: str) -> bool:
    return day in ["Sat", "Sun"]


def recommended_trucks(service_day: str) -> int:
    return WEEKEND_TRUCKS if is_weekend(service_day) else WEEKDAY_TRUCKS


def build_osrm_table_url(coords: List[Tuple[float, float]]) -> str:
    coord_str = ";".join([f"{lon},{lat}" for lat, lon in coords])
    return f"{OSRM_BASE_URL}/table/v1/driving/{coord_str}?annotations=distance,duration"


def fetch_osrm_matrix(coords: List[Tuple[float, float]]) -> Tuple[List[List[float]], List[List[int]]]:
    url = build_osrm_table_url(coords)

    for attempt in range(OSRM_RETRIES):
        try:
            response = requests.get(url, timeout=OSRM_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if data.get("code") != "Ok":
                raise RuntimeError(f"OSRM returned code={data.get('code')}")

            distances_m = data["distances"]
            durations_s = data["durations"]

            distances_km = [[(d or 0) / 1000.0 for d in row] for row in distances_m]
            durations_min = [[int(round((d or 0) / 60.0)) for d in row] for row in durations_s]

            return distances_km, durations_min

        except Exception as e:
            if attempt == OSRM_RETRIES - 1:
                raise RuntimeError(f"OSRM failed after {OSRM_RETRIES} attempts: {e}")
            time.sleep(1.5 * (attempt + 1))

    raise RuntimeError("Unexpected OSRM failure")


def build_haversine_matrix(coords: List[Tuple[float, float]]) -> Tuple[List[List[float]], List[List[int]]]:
    n = len(coords)
    dist_matrix = [[0.0] * n for _ in range(n)]
    time_matrix = [[0] * n for _ in range(n)]

    for i in range(n):
        lat1, lon1 = coords[i]
        for j in range(n):
            if i == j:
                continue
            lat2, lon2 = coords[j]
            km = haversine_km(lat1, lon1, lat2, lon2)
            dist_matrix[i][j] = km
            time_matrix[i][j] = km_to_minutes(km)

    return dist_matrix, time_matrix


def route_distance_km(route_nodes: List[int], dist_matrix: List[List[float]]) -> float:
    total = 0.0
    for i in range(len(route_nodes) - 1):
        total += dist_matrix[route_nodes[i]][route_nodes[i + 1]]
    return total


def route_time_min(route_nodes: List[int], time_matrix: List[List[int]], service_times: List[int]) -> int:
    total = 0
    for i in range(len(route_nodes) - 1):
        frm = route_nodes[i]
        to = route_nodes[i + 1]
        total += time_matrix[frm][to]
        if to != 0:
            total += service_times[to]
    return total


def run_fill_prediction():
    subprocess.run(["python", PREDICTION_SCRIPT], check=True)


def load_data() -> pd.DataFrame:
    df = pd.read_csv(PREDICTION_OUTPUT)

    required_cols = [
        "bin_id",
        "location",
        "latitude",
        "longitude",
        "fill_pct",
        "predicted_fill_pct",
        "predicted_fill_l",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {PREDICTION_OUTPUT}: {missing}")

    if "service_time_min" not in df.columns:
        df["service_time_min"] = DEFAULT_SERVICE_TIME_MIN

    if "on_call" not in df.columns:
        df["on_call"] = False

    df["bin_id"] = df["bin_id"].apply(safe_int)
    df["latitude"] = df["latitude"].apply(safe_float)
    df["longitude"] = df["longitude"].apply(safe_float)
    df["fill_pct"] = df["fill_pct"].apply(safe_float).clip(0, 100)
    df["predicted_fill_pct"] = df["predicted_fill_pct"].apply(safe_float).clip(0, 100)
    df["predicted_fill_l"] = df["predicted_fill_l"].apply(safe_float).fillna(0).clip(lower=0)
    df["service_time_min"] = df["service_time_min"].apply(lambda x: safe_int(x, DEFAULT_SERVICE_TIME_MIN))

    return df


def add_estimated_loads(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    predicted_denominator = df["predicted_fill_pct"].replace(0, pd.NA)
    liters_per_percent = (df["predicted_fill_l"] / predicted_denominator).fillna(0)

    df["csv_fill_l"] = (df["fill_pct"] * liters_per_percent).fillna(0).clip(lower=0)
    df["predicted_fill_l"] = df["predicted_fill_l"].fillna(0).clip(lower=0)
    df["liters_per_percent"] = liters_per_percent

    return df


def load_live_readings() -> pd.DataFrame:
    live_path = Path(LIVE_READINGS_FILE)

    if not live_path.exists():
        return pd.DataFrame()

    try:
        with open(live_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return pd.DataFrame()

    rows = list(data.values())
    if not rows:
        return pd.DataFrame()

    live_df = pd.DataFrame(rows)

    if "bin_id" not in live_df.columns or "fill_percent" not in live_df.columns:
        return pd.DataFrame()

    live_df["bin_id"] = live_df["bin_id"].apply(safe_int)
    live_df["fill_percent"] = live_df["fill_percent"].apply(safe_float).clip(0, 100)

    if "battery_percent" in live_df.columns:
        live_df["battery_percent"] = live_df["battery_percent"].apply(safe_float)
    else:
        live_df["battery_percent"] = None

    if "status" not in live_df.columns:
        live_df["status"] = "unknown"

    if "received_at" in live_df.columns:
        live_df["received_at"] = pd.to_datetime(live_df["received_at"], utc=True, errors="coerce")
    else:
        live_df["received_at"] = pd.NaT

    now = pd.Timestamp.now(tz="UTC")
    live_df["age_min"] = (now - live_df["received_at"]).dt.total_seconds() / 60.0
    live_df["is_fresh"] = live_df["age_min"].notna() & (live_df["age_min"] <= LIVE_MAX_AGE_MIN)

    return live_df


def is_live_row_usable(row) -> bool:
    if not bool(row.get("live_is_fresh", False)):
        return False

    if pd.isna(row.get("live_fill_percent")):
        return False

    status = str(row.get("live_status", "unknown")).lower()
    if status == "critical":
        return False

    return True


def merge_live_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    live_df = load_live_readings()

    df["current_fill_pct"] = df["fill_pct"]
    df["current_fill_l"] = df["csv_fill_l"]
    df["fill_source"] = "csv"
    df["live_battery_percent"] = None
    df["live_status_out"] = None
    df["live_received_at"] = pd.NaT

    if live_df.empty:
        return df

    live_df = live_df.rename(columns={
        "fill_percent": "live_fill_percent",
        "battery_percent": "live_battery_percent",
        "status": "live_status",
        "received_at": "live_received_at",
        "age_min": "live_age_min",
        "is_fresh": "live_is_fresh",
    })

    live_cols = [
        "bin_id",
        "live_fill_percent",
        "live_battery_percent",
        "live_status",
        "live_received_at",
        "live_age_min",
        "live_is_fresh",
    ]
    merged = df.merge(live_df[live_cols], on="bin_id", how="left")

    usable_mask = merged.apply(is_live_row_usable, axis=1)

    merged.loc[usable_mask, "current_fill_pct"] = merged.loc[usable_mask, "live_fill_percent"].clip(0, 100)
    merged.loc[usable_mask, "current_fill_l"] = (
        merged.loc[usable_mask, "current_fill_pct"] * merged.loc[usable_mask, "liters_per_percent"]
    ).fillna(0).clip(lower=0)
    merged.loc[usable_mask, "fill_source"] = "live"

    merged["live_status_out"] = merged["live_status"]

    merged["current_fill_pct"] = merged["current_fill_pct"].fillna(merged["fill_pct"]).clip(0, 100)
    merged["current_fill_l"] = merged["current_fill_l"].fillna(merged["csv_fill_l"]).clip(lower=0)

    drop_cols = ["live_age_min", "live_is_fresh"]
    merged = merged.drop(columns=[c for c in drop_cols if c in merged.columns])

    return merged


def filter_for_service_day(df: pd.DataFrame, service_day: str) -> pd.DataFrame:
    service_day = service_day.strip()
    if not service_day:
        return df.copy()

    if "days_csv" not in df.columns:
        return df.copy()

    def day_matches(val: str) -> bool:
        if pd.isna(val):
            return False
        val = str(val).strip()
        if val == "":
            return False
        if val in ["Weekly", "Monthly"]:
            return False
        return service_day in [x.strip() for x in val.split(",")]

    scheduled = df[df["days_csv"].apply(day_matches)].copy()
    on_call = df[df["on_call"].astype(str).str.lower().eq("true")].copy()

    if on_call.empty:
        return scheduled

    combined = pd.concat([scheduled, on_call], ignore_index=True).drop_duplicates(subset=["bin_id"])
    return combined


def apply_special_event_flags(df: pd.DataFrame, special_event: str) -> pd.DataFrame:
    df = df.copy()

    if "location_type" not in df.columns:
        df["location_type"] = ""

    if special_event == "game_day":
        athletic_mask = df["location_type"].astype(str).eq("athletics")
        dining_mask = df["location_type"].astype(str).eq("dining_hall")
        df.loc[athletic_mask | dining_mask, "predicted_fill_pct"] += 8
    elif special_event == "move_in":
        residential_mask = df["location_type"].astype(str).eq("residential")
        dining_mask = df["location_type"].astype(str).eq("dining_hall")
        df.loc[residential_mask | dining_mask, "predicted_fill_pct"] += 10
    elif special_event == "move_out":
        residential_mask = df["location_type"].astype(str).eq("residential")
        df.loc[residential_mask, "predicted_fill_pct"] += 10

    df["predicted_fill_pct"] = df["predicted_fill_pct"].clip(0, 100)

    base_lpp = df["liters_per_percent"] if "liters_per_percent" in df.columns else 0
    df["predicted_fill_l"] = (df["predicted_fill_pct"] * base_lpp).fillna(df["predicted_fill_l"]).clip(lower=0)
    df["liters_per_percent"] = base_lpp

    if "csv_fill_l" in df.columns:
        df["csv_fill_l"] = (df["fill_pct"] * df["liters_per_percent"]).fillna(0).clip(lower=0)

    return df


def select_bins(df: pd.DataFrame, urgent_ids: List[int]) -> pd.DataFrame:
    urgent_set = set(urgent_ids)
    df = df.copy()

    def reason(row):
        if row["bin_id"] in urgent_set:
            return "urgent_override"
        if row["fill_source"] == "live" and row["current_fill_pct"] >= FILL_THRESHOLD:
            return "live_fill_above_threshold"
        if row["fill_pct"] >= FILL_THRESHOLD:
            return "current_csv_fill_above_threshold"
        if row["predicted_fill_pct"] >= FILL_THRESHOLD:
            return "predicted_fill_above_threshold"
        return "not_selected"

    df["selection_reason"] = df.apply(reason, axis=1)
    df["selected_for_pickup"] = df["selection_reason"] != "not_selected"
    df["pickup_load_l"] = 0.0

    csv_mask = df["selection_reason"] == "current_csv_fill_above_threshold"
    live_mask = df["selection_reason"] == "live_fill_above_threshold"
    pred_mask = df["selection_reason"] == "predicted_fill_above_threshold"
    urgent_mask = df["selection_reason"] == "urgent_override"

    df.loc[csv_mask, "pickup_load_l"] = df.loc[csv_mask, "csv_fill_l"]
    df.loc[live_mask, "pickup_load_l"] = df.loc[live_mask, "current_fill_l"]
    df.loc[pred_mask, "pickup_load_l"] = df.loc[pred_mask, "predicted_fill_l"]
    df.loc[urgent_mask, "pickup_load_l"] = df.loc[urgent_mask, [
        "current_fill_l", "csv_fill_l", "predicted_fill_l"
    ]].max(axis=1)

    df["pickup_load_l"] = df["pickup_load_l"].fillna(0).clip(lower=0)

    selected = df[df["selected_for_pickup"]].copy()
    selected = selected[selected["pickup_load_l"] > 0].copy()

    return selected


def split_remote_bins(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    campus_rows = []
    remote_rows = []

    for _, row in df.iterrows():
        km = haversine_km(DEPOT_LAT, DEPOT_LON, row["latitude"], row["longitude"])
        if km > REMOTE_BIN_DISTANCE_KM:
            remote_rows.append(row)
        else:
            campus_rows.append(row)

    campus_df = pd.DataFrame(campus_rows).reset_index(drop=True)
    remote_df = pd.DataFrame(remote_rows).reset_index(drop=True)
    return campus_df, remote_df


def solve_balanced_routes(selected_bins: pd.DataFrame, num_trucks: int) -> Optional[Dict]:
    if selected_bins.empty:
        return None

    coords = [(DEPOT_LAT, DEPOT_LON)] + list(zip(selected_bins["latitude"], selected_bins["longitude"]))
    service_times = [0] + selected_bins["service_time_min"].astype(int).tolist()
    demands = [0] + selected_bins["pickup_load_l"].round().astype(int).tolist()
    labels = [DEPOT_NAME] + selected_bins["bin_id"].astype(str).tolist()

    print(f"  Fetching road distances from OSRM for {len(coords) - 1} locations...")

    using_haversine = False
    try:
        dist_matrix, time_matrix = fetch_osrm_matrix(coords)
    except Exception:
        print("  Warning: OSRM request timed out, falling back to haversine")
        dist_matrix, time_matrix = build_haversine_matrix(coords)
        using_haversine = True
        print("  Using haversine distance (straight-line fallback)")

    manager = pywrapcp.RoutingIndexManager(len(coords), num_trucks, 0)
    routing = pywrapcp.RoutingModel(manager)

    def time_callback(from_index, to_index):
        from_node = manager.IndexToNode(from_index)
        to_node = manager.IndexToNode(to_index)
        travel = time_matrix[from_node][to_node]
        service = service_times[to_node] if to_node != 0 else 0
        return int(travel + service)

    transit_callback_index = routing.RegisterTransitCallback(time_callback)
    routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

    routing.AddDimension(
        transit_callback_index,
        0,
        MAX_ROUTE_MIN,
        True,
        "Time"
    )
    time_dimension = routing.GetDimensionOrDie("Time")
    time_dimension.SetGlobalSpanCostCoefficient(100)

    def demand_callback(from_index):
        from_node = manager.IndexToNode(from_index)
        return int(demands[from_node])

    demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)

    routing.AddDimensionWithVehicleCapacity(
        demand_callback_index,
        0,
        [VEHICLE_CAPACITY_L] * num_trucks,
        True,
        "Capacity"
    )

    for node in range(1, len(coords)):
        routing.AddDisjunction([manager.NodeToIndex(node)], 100000)

    search_params = pywrapcp.DefaultRoutingSearchParameters()
    search_params.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
    search_params.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
    search_params.time_limit.seconds = 10

    solution = routing.SolveWithParameters(search_params)
    if not solution:
        return None

    routes = []
    total_distance = 0.0
    total_time = 0
    total_load = 0

    for vehicle_id in range(num_trucks):
        index = routing.Start(vehicle_id)
        route_nodes = []
        route_labels = []
        route_load = 0

        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            route_nodes.append(node)
            route_labels.append(labels[node])
            if node != 0:
                route_load += demands[node]
            index = solution.Value(routing.NextVar(index))

        end_node = manager.IndexToNode(index)
        route_nodes.append(end_node)
        route_labels.append(labels[end_node])

        visited_non_depot = [n for n in route_nodes if n != 0]
        if not visited_non_depot:
            continue

        dist_km = route_distance_km(route_nodes, dist_matrix)
        time_min = route_time_min(route_nodes, time_matrix, service_times)

        total_distance += dist_km
        total_time += time_min
        total_load += route_load

        routes.append({
            "vehicle_id": vehicle_id,
            "route_labels": route_labels,
            "time_min": time_min,
            "distance_km": dist_km,
            "load_l": route_load,
        })

    return {
        "routes": routes,
        "total_distance_km": total_distance,
        "total_time_min": total_time,
        "total_load_l": total_load,
        "using_haversine": using_haversine,
    }


def build_remote_routes(remote_bins: pd.DataFrame, start_vehicle_id: int) -> List[Dict]:
    routes = []

    for i, (_, row) in enumerate(remote_bins.iterrows()):
        vehicle_id = start_vehicle_id + i
        one_way_km = haversine_km(DEPOT_LAT, DEPOT_LON, row["latitude"], row["longitude"])
        round_trip_km = 2 * one_way_km
        travel_min = km_to_minutes(round_trip_km)
        service_min = safe_int(row.get("service_time_min", DEFAULT_SERVICE_TIME_MIN), DEFAULT_SERVICE_TIME_MIN)
        total_min = travel_min + service_min

        routes.append({
            "vehicle_id": vehicle_id,
            "route_labels": [DEPOT_NAME, str(safe_int(row["bin_id"])), DEPOT_NAME],
            "time_min": total_min,
            "distance_km": round_trip_km,
            "load_l": safe_int(round(row["pickup_load_l"])),
            "remote_location": row["location"],
        })

    return routes


def estimate_fuel_l(total_distance_km: float) -> float:
    return total_distance_km * 0.395


def print_header():
    print("=" * 80)
    print("MERIDIAN WASTE MANAGEMENT - ROUTE OPTIMIZATION")
    print("Virginia Tech Campus - S26-21")
    print("=" * 80)
    print(f"Mode: {MODE}")
    print("  - Minimizes collection time (less hours)")
    print("  - Ensures fair workload distribution between drivers")
    print("  - Creates geographically coherent routes (less crossover)")
    print("  - Reduces fuel consumption\n")
    print("Meridian Operations:")
    print("  - Routes scheduled day before")
    print("  - Can accommodate real-time dispatch changes")
    print("  - Weekdays (Mon-Fri): 7 trucks available")
    print("  - Weekends (Sat-Sun): 1-2 trucks available\n")


def print_selection_summary(total_bins: int, filtered_bins: int, selected_df: pd.DataFrame, remote_df: pd.DataFrame):
    current_count = int((selected_df["selection_reason"] == "current_csv_fill_above_threshold").sum()) if not selected_df.empty else 0
    live_count = int((selected_df["selection_reason"] == "live_fill_above_threshold").sum()) if not selected_df.empty else 0
    pred_only_count = int((selected_df["selection_reason"] == "predicted_fill_above_threshold").sum()) if not selected_df.empty else 0
    urgent_count = int((selected_df["selection_reason"] == "urgent_override").sum()) if not selected_df.empty else 0
    total_load = int(round(selected_df["pickup_load_l"].sum())) if not selected_df.empty else 0

    print("\n=== BIN SELECTION ===")
    print(f"  Total bins in system: {total_bins}")
    print(f"  Bins considered after day filter: {filtered_bins}")
    print(f"  {current_count} bins selected by CSV current fill (≥{FILL_THRESHOLD:.1f}%)")
    print(f"  {live_count} bins selected by live TTN data")
    print(f"  {pred_only_count} bins selected by prediction")
    print(f"  {urgent_count} bins selected by urgent override")
    print(f"  Final selection: {len(selected_df)} bins, {total_load} L total")

    if not remote_df.empty:
        print(f"  Remote bins (>{REMOTE_BIN_DISTANCE_KM:.1f} km, dedicated truck each): {remote_df['location'].tolist()}")


def print_debug_selected_bins(selected_df: pd.DataFrame):
    if selected_df.empty:
        print("\nDEBUG SELECTED BINS: none")
        return

    debug_cols = [
        "bin_id",
        "location",
        "fill_pct",
        "predicted_fill_pct",
        "current_fill_pct",
        "csv_fill_l",
        "predicted_fill_l",
        "current_fill_l",
        "pickup_load_l",
        "selection_reason",
        "fill_source",
        "live_fill_percent",
        "live_status",
    ]
    cols_to_show = [c for c in debug_cols if c in selected_df.columns]
    print("\nDEBUG SELECTED BINS:")
    print(selected_df[cols_to_show].to_string(index=False))


def print_routes(campus_result: Optional[Dict], remote_routes: List[Dict]):
    print(f"\n=== ROUTES ({MODE}) ===\n")

    campus_routes = campus_result["routes"] if campus_result else []
    all_routes = campus_routes + remote_routes

    for route in all_routes:
        labels = " -> ".join(route["route_labels"])
        if "remote_location" in route:
            print(f"Truck {route['vehicle_id']} [REMOTE]: {labels}")
        else:
            print(f"Truck {route['vehicle_id']}: {labels}")
        print(f"  Time: {route['time_min']} min | Distance: {route['distance_km']:.2f} km | Load: {route['load_l']} L\n")

    total_time = (campus_result["total_time_min"] if campus_result else 0) + sum(r["time_min"] for r in remote_routes)
    total_distance = (campus_result["total_distance_km"] if campus_result else 0.0) + sum(r["distance_km"] for r in remote_routes)
    total_fuel = estimate_fuel_l(total_distance)

    campus_times = [r["time_min"] for r in campus_routes]
    balance_text = "N/A"
    if campus_times:
        min_t = min(campus_times)
        max_t = max(campus_times)
        diff = max_t - min_t
        balance_text = f"min={min_t} max={max_t} diff={diff} min"
        balance_text += " ✓ Well-balanced" if diff <= 15 else " ⚠ Needs improvement"

    print("=== SUMMARY ===")
    print(f"Total time: {int(round(total_time))} min")
    print(f"Total distance: {total_distance:.2f} km")
    print(f"Total fuel: {total_fuel:.2f} L")
    print(f"Campus balance: {balance_text}")

    for r in remote_routes:
        print(
            f"Remote truck {r['vehicle_id']}: {r['time_min']} min ({r['distance_km']:.1f} km) — "
            "dedicated long-haul, excluded from balance check"
        )

    print("\n" + "=" * 80)
    print("SUCCESS - Routes generated")
    print("=" * 80)


def get_service_day_input() -> str:
    service_day = input("Service day (Mon/Tue/Wed/Thu/Fri/Sat/Sun) or Enter for ALL: ").strip()
    if service_day and service_day not in DAY_ORDER:
        print("Invalid service day. Using ALL.")
        service_day = ""
    return service_day


def get_special_event_input() -> str:
    special_event = input("Special event? (move_in/move_out/game_day/none): ").strip() or "none"
    if special_event not in {"move_in", "move_out", "game_day", "none"}:
        special_event = "none"
    return special_event


def get_num_trucks_input(default_trucks: int) -> int:
    raw = input(f"Number of trucks (or Enter for {default_trucks}): ").strip()
    if not raw:
        return default_trucks
    try:
        val = int(raw)
        return max(1, val)
    except Exception:
        return default_trucks


def get_urgent_bins_input() -> List[int]:
    urgent_ids = []
    add_urgent = input("Add urgent bin for immediate pickup? [y/N]: ").strip().lower()
    while add_urgent == "y":
        raw = input("  Enter urgent bin_id: ").strip()
        try:
            urgent_ids.append(int(raw))
        except Exception:
            print("  Invalid bin_id, skipping.")
        add_urgent = input("Add another urgent bin? [y/N]: ").strip().lower()
    return urgent_ids


def cli():
    print_header()

    service_day = get_service_day_input()
    default_trucks = recommended_trucks(service_day) if service_day else WEEKDAY_TRUCKS
    print(f"Recommended trucks for {'all days' if not service_day else service_day}: {default_trucks}")

    special_event = get_special_event_input()
    num_trucks = get_num_trucks_input(default_trucks)
    urgent_ids = get_urgent_bins_input()

    run_fill_prediction()

    full_df = load_data()
    full_df = add_estimated_loads(full_df)
    total_bins = len(full_df)

    day_filtered_df = filter_for_service_day(full_df, service_day)
    day_filtered_df = apply_special_event_flags(day_filtered_df, special_event)
    day_filtered_df = merge_live_data(day_filtered_df)

    selected_df = select_bins(day_filtered_df, urgent_ids)
    campus_df, remote_df = split_remote_bins(selected_df)

    print_selection_summary(
        total_bins=total_bins,
        filtered_bins=len(day_filtered_df),
        selected_df=selected_df,
        remote_df=remote_df
    )

    print_debug_selected_bins(selected_df)

    if selected_df.empty:
        print("\nNo bins selected for pickup.")
        print("\n" + "=" * 80)
        print("SUCCESS - No routes needed")
        print("=" * 80)
        return

    campus_trucks = min(max(1, num_trucks - len(remote_df)), max(1, len(campus_df)))
    campus_result = None

    if not campus_df.empty:
        campus_result = solve_balanced_routes(campus_df, campus_trucks)
        if campus_result is None:
            print("\nCould not generate campus routes with current constraints.")
            return

    remote_start_id = len(campus_result["routes"]) if campus_result else 0
    remote_routes = build_remote_routes(remote_df, remote_start_id)

    # Renumber all routes consecutively — solver may leave gaps from empty trucks
    all_routes = (campus_result["routes"] if campus_result else []) + remote_routes
    for i, route in enumerate(all_routes):
        route["vehicle_id"] = i
    if campus_result:
        campus_result["routes"] = all_routes[:len(campus_result["routes"])]
    remote_routes = all_routes[len(campus_result["routes"]) if campus_result else 0:]

    print_routes(campus_result, remote_routes)


if __name__ == "__main__":
    cli()
