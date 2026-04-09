import folium
import pandas as pd
from typing import List, Dict, Any


def build_route_map(
    routes: List[Dict[str, Any]],
    bins_df: pd.DataFrame,
    output_html: str = "miami_routes_demo.html",
):
    """
    Build an interactive folium map showing routes for each truck.

    routes: list of dicts from solve_routes(), e.g.
        {
            "truck_id": 0,
            "stops": [194, 120, 109, ...],
            "total_time_min": 241,
            "total_load_l": 7920
        }

    bins_df: DataFrame with at least columns:
        bin_id, lat, lon

    output_html: filename of the saved map.
    """

    if not routes:
        print("No routes to plot.")
        return

    # Make sure bin_id is the index for quick lookup
    if "bin_id" not in bins_df.columns:
        raise ValueError("bins_df must contain a 'bin_id' column.")
    bins_df = bins_df.set_index("bin_id")

    # Collect all coordinates used in routes to determine a map center
    all_coords = []
    for r in routes:
        for bin_id in r["stops"]:
            if bin_id in bins_df.index:
                row = bins_df.loc[bin_id]
                all_coords.append((row["lat"], row["lon"]))

    if not all_coords:
        print("No matching bin coordinates found for route stops.")
        return

    center_lat = sum(lat for lat, _ in all_coords) / len(all_coords)
    center_lon = sum(lon for _, lon in all_coords) / len(all_coords)

    # Create base map centered on the bins
    m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

    # Color palette for different trucks
    colors = [
        "red",
        "blue",
        "green",
        "purple",
        "orange",
        "darkred",
        "lightblue",
        "lightgreen",
    ]

    for route in routes:
        truck_id = route["truck_id"]
        color = colors[truck_id % len(colors)]

        # Build ordered list of coordinates for this truck's stops
        coords = []
        for bin_id in route["stops"]:
            if bin_id not in bins_df.index:
                continue
            row = bins_df.loc[bin_id]
            coords.append((row["lat"], row["lon"]))

        if not coords:
            continue

        # Draw polyline for this truck's route
        folium.PolyLine(
            locations=coords,
            color=color,
            weight=4,
            opacity=0.8,
            tooltip=f"Truck {truck_id}",
        ).add_to(m)

        # Add small markers for each stop
        for i, (lat, lon) in enumerate(coords):
            folium.CircleMarker(
                location=[lat, lon],
                radius=3,
                color=color,
                fill=True,
                fill_opacity=0.9,
                tooltip=f"Truck {truck_id} – Stop {i + 1}",
            ).add_to(m)

    m.save(output_html)
    print(f"Route map saved to {output_html}")
