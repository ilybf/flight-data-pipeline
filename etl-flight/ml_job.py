import pandas as pd
import pickle
from sqlalchemy import create_engine

# 1. Setup Engine
engine = create_engine('postgresql://postgres:postgres@dwh-postgres:5432/dwh_database')

def run_flight_delay_prediction():
    print("--- Starting Prediction Job ---")

    # 2. Load Model
    try:
        latest_version = pd.read_sql(
            "SELECT model_version FROM model_training_log ORDER BY training_timestamp DESC LIMIT 1",
            engine
        )["model_version"].iloc[0]
        
        print(f"Loading model version: {latest_version}")
        clf = pickle.load(open(f"delay_classifier_{latest_version}.pkl", "rb"))
        reg = pickle.load(open(f"delay_regressor_{latest_version}.pkl", "rb"))
    except Exception as e:
        print(f"ERROR: Could not load model. Reason: {e}")
        return

    # 3. Get Data
    query = """
    SELECT
        f.id AS flight_id,
        f.day_of_week,
        f.distance,
        f.scheduled_departure,
        f.carrier_name,
        f.origin_airport,
        w.temperature_c,
        w.precipitation_mm,
        w.wind_speed_kph
    FROM gold_dim_flight f
    LEFT JOIN gold_dim_weather w 
        ON f.origin_airport = w.airport_iata
       AND f.day = w.date_id
    WHERE f.cancelled = FALSE AND f.diverted = FALSE;
    """

    df = pd.read_sql(query, engine)
    print(f"Rows fetched from DB: {len(df)}")

    if df.empty:
        print("WARNING: No flights found to predict. Stopping.")
        return

    # 4. Handle Missing Data (CRITICAL FIX)
    # Instead of dropping everything, let's fill missing weather with averages or 0
    # OR: checking how many rows we lose
    initial_count = len(df)
    df.dropna(inplace=True) 
    final_count = len(df)
    
    print(f"Rows after dropping NULLs: {final_count} (Dropped {initial_count - final_count} rows)")

    if df.empty:
        print("ERROR: All rows were dropped! Check your Weather table joins.")
        return

    # 5. Feature Engineering (MUST MATCH TRAINING)
    # Convert Timestamp to Numbers
    df['dep_hour'] = pd.to_datetime(df['scheduled_departure']).dt.hour
    df['dep_minute'] = pd.to_datetime(df['scheduled_departure']).dt.minute

    # Select exact columns used in training
    # Note: 'scheduled_departure' is REMOVED, 'dep_hour'/'dep_minute' ADDED
    X = df[[
        "day_of_week", "distance", "dep_hour", "dep_minute",
        "carrier_name", "origin_airport",
        "temperature_c", "precipitation_mm", "wind_speed_kph"
    ]]

    # 6. Predict
    try:
        df["prediction_delay_probability"] = clf.predict_proba(X)[:,1]
        df["predicted_delay_minutes"] = reg.predict(X)
        df["model_version"] = latest_version
    except ValueError as e:
        print(f"ERROR during prediction: {e}")
        print("Tip: Ensure the columns in 'X' exactly match the columns used in training.")
        return

    # 7. Write to DB
    df_to_write = df[["flight_id", "prediction_delay_probability",
                      "predicted_delay_minutes", "model_version"]]

    df_to_write.to_sql("gold_flight_delay_prediction",
                       engine, if_exists="append", index=False)

    print(f"SUCCESS: Predictions inserted: {len(df_to_write)}")

if __name__ == "__main__":
    run_flight_delay_prediction()