import joblib
import numpy as np
import pandas as pd

HISTORICAL_DATA_PATH = "historical_fill_data.csv"
CURRENT_BINS_PATH = "MeridianData.csv"
MODEL_PATH = "fill_level_model.pkl"
OUTPUT_PATH = "MeridianData_with_predictions.csv"

FILL_THRESHOLD = 80.0

BIN_CAPACITY_L = {
    "compactor": 350,
    "standard": 300,
    "20yd": 1200,
    "30yd": 1800,
    "34yd_compactor": 2200,
    "metal": 900,
    "OCC": 650,
}

VT_LOCATION_COORDS = {
    "The Inn": (37.2287, -80.4217),
    "West End Market (Dining Hall)": (37.2268, -80.4265),
    "Dietrich D2 (Dining Hall)": (37.2282, -80.4241),
    "Owens (Dining Hall)": (37.2267, -80.4186),
    "Lavery/Turner Place (Dining Hall)": (37.2254, -80.4198),
    "Southgate Center": (37.2187, -80.4164),
    "Lee/Pritchard": (37.2292, -80.4235),
    "A J Hall": (37.2251, -80.4190),
    "Squires": (37.2294, -80.4185),
    "Vet Med (Straw Beside Bldg)": (37.2200, -80.4278),
    "Vet Med (Trash beside dock Pan #205) 20yd": (37.2200, -80.4278),
    "Vet Med (Trash @ loading dock Pan #245)": (37.2200, -80.4278),
    "Plant Pathology": (37.2213, -80.4208),
    "Kentland Farms (Dairy Barn)": (37.1975, -80.5805),
    "Civil Engineering": (37.2290, -80.4240),
    "The Wall (Trash)": (37.2311, -80.4206),
    "Durham Hall (Metal)": (37.2285, -80.4209),
    "Electric Service": (37.2129, -80.4105),
    "The Wall (Recycling)": (37.2311, -80.4206),
    "Cowgill": (37.2271, -80.4228),
    "Litton Reeves": (37.2270, -80.4155),
    "Torgeson": (37.2291, -80.4199),
    "Merryman - VT Athletics": (37.2200, -80.4163),
    "Hitt Hall": (37.2302, -80.4282),
    "Bishop Favro": (37.2244, -80.4265),
    "Lavery - OCC": (37.2254, -80.4198),
    "Owens - OCC": (37.2267, -80.4186),
    "D2 - OCC": (37.2282, -80.4241),
    "Overflow - OCC": (37.2260, -80.4220),
    "CLMS": (37.2274, -80.4220),
}


def get_coordinates(location_name: str):
    if location_name in VT_LOCATION_COORDS:
        return VT_LOCATION_COORDS[location_name]

    if "vet med" in str(location_name).lower():
        return VT_LOCATION_COORDS["Vet Med (Straw Beside Bldg)"]

    raise ValueError(f"No coordinates found for location: {location_name}")


def build_prediction_frame(history_df: pd.DataFrame, current_bins_df: pd.DataFrame) -> pd.DataFrame:
    history_df = history_df.copy()
    current_bins_df = current_bins_df.copy()

    history_df["date"] = pd.to_datetime(history_df["date"])
    current_bins_df["bin_id"] = current_bins_df["vt_id"].astype(int)

    latest = (
        history_df.sort_values(["bin_id", "date"])
        .groupby("bin_id", as_index=False)
        .tail(1)[
            [
                "bin_id",
                "day_of_week",
                "month",
                "is_weekend",
                "is_game_day",
                "is_move_in",
                "is_move_out",
                "days_since_last_service",
                "service_performed",
                "fill_pct_start_of_day",
                "daily_fill_added_pct",
                "fill_pct",
                "fill_l",
                "fill_pct_lag_1",
                "fill_pct_lag_2",
                "fill_pct_lag_3",
                "rolling_mean_3",
            ]
        ]
        .copy()
    )

    merged = current_bins_df.merge(latest, on="bin_id", how="left")

    merged["on_call"] = merged["on_call"].astype(str).str.lower().eq("true")
    merged["eow"] = merged["eow"].astype(str).str.lower().eq("true")

    merged["location_type"] = merged["location_type"].fillna("standard")
    merged["service_time_min"] = merged["service_time_min"].fillna(5)
    merged["bin_type"] = merged["bin_type"].fillna("compactor")

    merged["day_of_week"] = merged["day_of_week"].fillna("Mon")
    merged["month"] = merged["month"].fillna(4)
    merged["is_weekend"] = merged["is_weekend"].fillna(0)
    merged["is_game_day"] = merged["is_game_day"].fillna(0)
    merged["is_move_in"] = merged["is_move_in"].fillna(0)
    merged["is_move_out"] = merged["is_move_out"].fillna(0)
    merged["days_since_last_service"] = merged["days_since_last_service"].fillna(1)
    merged["service_performed"] = merged["service_performed"].fillna(0)

    merged["fill_pct_start_of_day"] = merged["fill_pct_start_of_day"].fillna(25)
    merged["daily_fill_added_pct"] = merged["daily_fill_added_pct"].fillna(8)
    merged["fill_pct"] = merged["fill_pct"].fillna(35)
    merged["fill_l"] = merged["fill_l"].fillna(100)
    merged["fill_pct_lag_1"] = merged["fill_pct_lag_1"].fillna(30)
    merged["fill_pct_lag_2"] = merged["fill_pct_lag_2"].fillna(28)
    merged["fill_pct_lag_3"] = merged["fill_pct_lag_3"].fillna(25)
    merged["rolling_mean_3"] = merged["rolling_mean_3"].fillna(30)

    coords = merged["location"].apply(get_coordinates)
    merged["latitude"] = coords.apply(lambda x: x[0])
    merged["longitude"] = coords.apply(lambda x: x[1])

    return merged


def add_selection_logic(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["capacity_l"] = df["bin_type"].map(BIN_CAPACITY_L).fillna(350)
    df["current_fill_l"] = (df["fill_pct"] / 100.0) * df["capacity_l"]
    df["predicted_fill_l"] = (df["predicted_fill_pct"] / 100.0) * df["capacity_l"]

    def selection_reason(row):
        if row["fill_pct"] >= FILL_THRESHOLD:
            return "current_fill_above_threshold"
        if row["predicted_fill_pct"] >= FILL_THRESHOLD:
            return "predicted_fill_above_threshold"
        return "not_selected"

    df["selection_reason"] = df.apply(selection_reason, axis=1)

    # Keep these as real Python bools so tests using `is True` / `is False` pass
    df["selected_for_pickup"] = (
        df["selection_reason"]
        .map(lambda x: False if x == "not_selected" else True)
        .astype(object)
    )

    return df


def main():
    model = joblib.load(MODEL_PATH)
    history_df = pd.read_csv(HISTORICAL_DATA_PATH)
    current_bins_df = pd.read_csv(CURRENT_BINS_PATH)

    df = build_prediction_frame(history_df, current_bins_df)

    feature_cols = [
        "bin_id",
        "location_type",
        "on_call",
        "eow",
        "service_time_min",
        "bin_type",
        "day_of_week",
        "month",
        "is_weekend",
        "is_game_day",
        "is_move_in",
        "is_move_out",
        "days_since_last_service",
        "service_performed",
        "fill_pct_start_of_day",
        "daily_fill_added_pct",
        "fill_pct",
        "fill_l",
        "fill_pct_lag_1",
        "fill_pct_lag_2",
        "fill_pct_lag_3",
        "rolling_mean_3",
    ]

    preds = model.predict(df[feature_cols])
    df["predicted_fill_pct"] = np.array(preds).clip(0, 100)

    df = add_selection_logic(df)

    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Created {OUTPUT_PATH}")
    print(
        df[
            [
                "bin_id",
                "location",
                "latitude",
                "longitude",
                "fill_pct",
                "predicted_fill_pct",
                "predicted_fill_l",
                "selection_reason",
            ]
        ].head(10)
    )


if __name__ == "__main__":
    main()