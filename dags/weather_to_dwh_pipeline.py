from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pyodbc
import time


# =============== 🔹 CONNECTIONS ===============

def connect_to_db(db_name):
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=host.docker.internal,1433;"
        f"DATABASE={db_name};"
        "UID=pro;"
        "PWD=9512;"
        "TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return conn, cursor


# =============== 🔹 EXTRACT ===============

def extract_from_operational():
    conn, cursor = connect_to_db("WeatherDataDB")

    cursor.execute("""
        SELECT 
            w.WeatherID,
            c.City, c.Country, c.Latitude, c.Longitude,
            wc.ConditionID, wc.ConditionName, wc.Description,
            w.temperature, w.humidity, w.wind_speed, w.wind_direction,
            w.pressure, w.visibility, w.cloud_coverage, 
            w.Observation_Datetime, w.Model_num
        FROM WeatherObservation w
        JOIN City c ON w.CityID = c.CityID
        JOIN WeatherCondition wc ON w.ConditionID = wc.ConditionID
    """)

    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    conn.close()

    data = [dict(zip(columns, row)) for row in rows]
    print(f"✅ Extracted {len(data)} records from WeatherDataDB")
    return data


# =============== 🔹 TRANSFORM + LOAD ===============

def load_to_warehouse():
    operational_data = extract_from_operational()
    if not operational_data:
        print("⚠️ No data found to load.")
        return

    conn_wh, cur_wh = connect_to_db("project")

    for record in operational_data:
        # === DimLocation ===
        cur_wh.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimLocation WHERE city = ? AND country = ?)
                INSERT INTO DimLocation (country, city, latitude, longitude)
                VALUES (?, ?, ?, ?);
        """, (
            record["City"], record["Country"],
            record["Country"], record["City"],
            record["Latitude"], record["Longitude"]
        ))
        conn_wh.commit()

        cur_wh.execute("SELECT location_id FROM DimLocation WHERE city = ? AND country = ?",
                       (record["City"], record["Country"]))
        location_id = cur_wh.fetchone()[0]

        # === DimWeatherEvent ===
        cur_wh.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimWeatherEvent WHERE event_id = ?)
                INSERT INTO DimWeatherEvent (event_id, Weather_condition, description, severity)
                VALUES (?, ?, ?, 'Normal');
        """, (
            record["ConditionID"],
            record["ConditionID"], record["ConditionName"], record["Description"]
        ))
        conn_wh.commit()

        event_id = record["ConditionID"]

        # === DimTime ===
        obs_datetime = record["Observation_Datetime"]
        obs_date = obs_datetime.date()
        obs_time = obs_datetime.time()
        month = obs_date.month
        season = (
            "Winter" if month in [12, 1, 2] else
            "Spring" if month in [3, 4, 5] else
            "Summer" if month in [6, 7, 8] else
            "Autumn"
        )

        cur_wh.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimTime WHERE time_date = ? AND time_value = ?)
                INSERT INTO DimTime (time_date, season, time_value)
                VALUES (?, ?, ?);
        """, (obs_date, obs_time, obs_date, season, obs_time))
        conn_wh.commit()

        cur_wh.execute("SELECT time_id FROM DimTime WHERE time_date = ? AND time_value = ?",
                       (obs_date, obs_time))
        time_id = cur_wh.fetchone()[0]

        # === DimForecastModel ===
        model_id = record["Model_num"]
        # 1 = OpenWeather / 2 = VisualCrossing
        if model_id == 1:
            model_name = "OpenWeather"
        else:
            model_name = "VisualCrossing"

        cur_wh.execute("""
            IF NOT EXISTS (SELECT 1 FROM DimForecastModel WHERE model_name = ?)
                INSERT INTO DimForecastModel (model_name, api_url, description)
                VALUES (?, ?, ?);
        """, (
            model_name,model_name,
            "https://api.openweathermap.org" if model_id == 1
            else "https://weather.visualcrossing.com",
            "Weather data provider"
        ))
        conn_wh.commit()

        cur_wh.execute("SELECT model_id FROM DimForecastModel WHERE model_name = ?", (model_name,))
        model_id_final = cur_wh.fetchone()[0]

        # === FactWeatherObservation ===
        cur_wh.execute("""
            INSERT INTO FactWeatherObservation
                (time_id, location_id, event_id, model_id,
                temperature, humidity, wind_speed, wind_direction,
                pressure, precipitation, visibility, cloud_coverage)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0.0, ?, ?);
        """, (
            time_id, location_id, event_id, model_id_final,
            record["temperature"], record["humidity"],
            record["wind_speed"], record["wind_direction"],
            record["pressure"], record["visibility"], record["cloud_coverage"]
        ))
        conn_wh.commit()

        print(f"✅ Inserted observation for {record['City']} ({model_name}) at {obs_datetime}")

    conn_wh.close()
    print("🎯 ETL process completed successfully!")


# =============== 🔹 AIRFLOW DAG ===============

default_args = {
    "owner": "Omar",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "weather_to_dwh_pipeline",
    default_args=default_args,
    schedule_interval="0 * * * *",  # كل ساعة
    catchup=False,
    max_active_runs=1,
    description="ETL pipeline to load data from WeatherDataDB to WeatherWarehouseDB",
)

task_load = PythonOperator(
    task_id="load_weather_data_to_dwh",
    python_callable=load_to_warehouse,
    dag=dag,
)
