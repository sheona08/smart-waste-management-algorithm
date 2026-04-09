import geopandas as gpd
import pandas as pd
import numpy as np
from shapely.geometry import Point


BIN_COUNT_PER_ROUTE = 40  # number of bins to sample inside each route polygon
SERVICE_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri"]


def _sample_points_in_polygon(poly, n_points, rng):
    """Sample up to n_points random points inside a polygon using rejection sampling."""
    minx, miny, maxx, maxy = poly.bounds
    points = []
    attempts = 0
    max_attempts = n_points * 50  # just in case polygon is very thin

    while len(points) < n_points and attempts < max_attempts:
        x = rng.uniform(minx, maxx)
        y = rng.uniform(miny, maxy)
        p = Point(x, y)
        if poly.contains(p):
            points.append(p)
        attempts += 1

    return points


def main():
    # Load route polygons
    routes = gpd.read_file("Trash_Routes.geojson")
    print(f"Loaded {len(routes)} route polygons")

    bins = []
    rng = np.random.default_rng(42)

    for idx, row in routes.iterrows():
        route_id = row.get("FID", idx)
        poly = row.geometry

        if poly is None or poly.is_empty:
            print(f"Route {route_id}: geometry missing/empty, skipping.")
            continue

        points = _sample_points_in_polygon(poly, BIN_COUNT_PER_ROUTE, rng)
        if not points:
            print(f"Route {route_id}: could not sample points inside polygon, skipping.")
            continue

        for p in points:
            lon = float(p.x)
            lat = float(p.y)

            capacity_l = int(rng.choice([240, 1100], p=[0.7, 0.3]))
            service_time_min = float(rng.uniform(2, 5))  # time to service this bin
            base_growth_l_per_day = float(rng.uniform(50, 150))
            noise_l = float(rng.normal(0, 20))
            days_since_collection = int(rng.integers(1, 5))

            raw_fill = base_growth_l_per_day * days_since_collection + noise_l
            fill_l_today = float(np.clip(raw_fill, 0, capacity_l))
            predicted_fill_pct = float((fill_l_today / capacity_l) * 100 if capacity_l > 0 else 0)

            bins.append(
                {
                    "bin_id": len(bins) + 1,
                    "lat": lat,
                    "lon": lon,
                    "route_id": route_id,
                    "capacity_l": capacity_l,
                    "service_time_min": round(service_time_min, 2),
                    "base_growth_l_per_day": round(base_growth_l_per_day, 2),
                    "noise_l": round(noise_l, 2),
                    "fill_l_today": round(fill_l_today, 2),
                    "predicted_fill_pct": round(predicted_fill_pct, 2),
                }
            )

    if not bins:
        raise RuntimeError("No bins were generated. Check your Trash_Routes.geojson file.")

    bins_df = pd.DataFrame(bins)

    # Assign a random service day (Mon–Fri) to each bin
    rng = np.random.default_rng(123)
    bins_df["service_day"] = rng.choice(SERVICE_DAYS, size=len(bins_df))

    # Reorder columns so tests and downstream code see a stable schema
    bins_df = bins_df[
        [
            "bin_id",
            "lat",
            "lon",
            "route_id",
            "service_day",
            "capacity_l",
            "service_time_min",
            "base_growth_l_per_day",
            "noise_l",
            "fill_l_today",
            "predicted_fill_pct",
        ]
    ]

    output_path = "sim_bins3.csv"
    bins_df.to_csv(output_path, index=False)
    print(f"Generated {output_path} with {len(bins_df)} bins.")


if __name__ == "__main__":
    main()
