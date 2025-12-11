import pandas as pd
import pickle
from sqlalchemy import create_engine, text
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, mean_absolute_error
from datetime import datetime
import sys

# 1. Database Connection
engine = create_engine('postgresql://postgres:postgres@dwh-postgres:5432/dwh_database')

query = """
SELECT
    f.id as flight_id,
    f.day_of_week,
    f.distance,
    f.scheduled_departure,
    f.carrier_name,
    f.origin_airport,
    w.temperature_c,
    w.precipitation_mm,
    w.wind_speed_kph,
    CASE WHEN f.departure_delay > 10 THEN 1 ELSE 0 END AS is_delayed,
    f.departure_delay AS delay_minutes
FROM gold_dim_flight f
LEFT JOIN gold_dim_weather w 
    ON f.origin_airport = w.airport_iata
   AND f.day = w.date_id
WHERE f.cancelled = FALSE AND f.diverted = FALSE;
"""

print("Fetching data...")
df = pd.read_sql(query, engine)
print(f"Total rows fetched: {len(df)}")

# --- CRITICAL FIX START ---
# 1. Check if data is empty immediately
if df.empty:
    print("No data found in database. Exiting job gracefully.")
    sys.exit(0)

# 2. Handle Missing Weather Data smartly (Don't drop the flight!)
# Fill missing numerical weather data with 0 or Mean
df["temperature_c"] = df["temperature_c"].fillna(df["temperature_c"].mean() if not df["temperature_c"].isnull().all() else 0)
df["precipitation_mm"] = df["precipitation_mm"].fillna(0)
df["wind_speed_kph"] = df["wind_speed_kph"].fillna(0)

# 3. Only drop rows if the TARGET (Delay info) or CRITICAL features are missing
df.dropna(subset=["is_delayed", "delay_minutes", "scheduled_departure"], inplace=True)

print(f"Rows available for training after cleanup: {len(df)}")

# 4. Safety Check for train_test_split
if len(df) < 5:
    print("Not enough data to train a model (need at least 5 rows). Exiting.")
    sys.exit(0)
# --- CRITICAL FIX END ---

# 2. Fix Types
df["is_delayed"] = df["is_delayed"].astype(int)

# --- FEATURE ENGINEERING ---
# Convert timestamp to numbers
df['dep_hour'] = pd.to_datetime(df['scheduled_departure']).dt.hour
df['dep_minute'] = pd.to_datetime(df['scheduled_departure']).dt.minute

# Select features
X = df[[
    "day_of_week", "distance", "dep_hour", "dep_minute", 
    "carrier_name", "origin_airport", 
    "temperature_c", "precipitation_mm", "wind_speed_kph"
]]

y_class = df["is_delayed"]
y_reg = df["delay_minutes"]

# 3. Pipeline Setup
cat_cols = ["carrier_name", "origin_airport", "day_of_week"]

preprocess = ColumnTransformer([
    ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols)
], remainder="passthrough")

clf_model = Pipeline([
    ("preprocess", preprocess),
    ("model", RandomForestClassifier(n_estimators=100, n_jobs=-1))
])

reg_model = Pipeline([
    ("preprocess", preprocess),
    ("model", RandomForestRegressor(n_estimators=100, n_jobs=-1))
])

# 4. Split and Train
print("Splitting data...")
X_train, X_test, y_class_train, y_class_test, y_reg_train, y_reg_test = train_test_split(
    X, y_class, y_reg, test_size=0.2, random_state=42
)

print(f"Training on {len(X_train)} rows...")
clf_model.fit(X_train, y_class_train)
reg_model.fit(X_train, y_reg_train)

# 5. Evaluate
# Handle edge case if X_test is empty (rare given the check above, but safe)
if len(X_test) > 0:
    accuracy = accuracy_score(y_class_test, clf_model.predict(X_test))
    mae = mean_absolute_error(y_reg_test, reg_model.predict(X_test))
else:
    accuracy = 0.0
    mae = 0.0

print("Classifier Accuracy:", accuracy)
print("Regressor MAE:", mae)

# 6. Log to DB
model_version = "v" + datetime.now().strftime("%Y%m%d_%H%M")

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS model_training_log (
            model_version VARCHAR(20),
            accuracy REAL,
            mae REAL,
            training_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """))

    conn.execute(
        text("INSERT INTO model_training_log (model_version, accuracy, mae) VALUES (:v, :a, :m)"),
        {"v": model_version, "a": float(accuracy), "m": float(mae)}
    )

# 7. Save Models
pickle.dump(clf_model, open(f"delay_classifier_{model_version}.pkl", "wb"))
pickle.dump(reg_model, open(f"delay_regressor_{model_version}.pkl", "wb"))

print(f"Model Trained & Saved Successfully as: {model_version}")