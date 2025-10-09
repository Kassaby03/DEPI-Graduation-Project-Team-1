from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta
import requests
import pyodbc
import os


# ================== Database Connection ==================
def connection():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=host.docker.internal,1433;"
        "DATABASE=project;"
        "UID=pro;"
        "PWD=9512;"
        "TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return conn, cursor


# ================== City List ==================
def cities_Names():
    return [
        "Cairo", "Alexandria", "New York", "Los Angeles", "London",
        "Berlin", "Paris", "Tokyo", "Osaka", "Sydney"
    ]


# ================== API Request (Fallback Logic) ==================
def API_Request(city_name, API_KEY_OWM, API_KEY_VC):
    
    res=[]
    url1 = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city_name}?unitGroup=metric&key={API_KEY_VC}&contentType=json"
    res1 = requests.get(url1, timeout=10)
    if res1.status_code == 200:
            res.append([res1.json(), "VC"]) 
   

   
    url2 = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={API_KEY_OWM}&units=metric"
    res2 = requests.get(url2, timeout=10)
    if res2.status_code == 200:
        res.append([res2.json(), "OWM"]) 
        return res
   

       
    print(f" No data for {city_name}")
    return None, None


# ================== Extraction ==================
def get_DimTime(data, source):
    try:
        if source == "OWM":
            dt_utc = data.get("dt", int(datetime.utcnow().timestamp()))
            tz_offset = data.get("timezone", 0)
            local_time = datetime.utcfromtimestamp(dt_utc) + timedelta(seconds=tz_offset)
        else:  # VisualCrossing
            local_time = datetime.strptime(data["days"][0]["datetime"], "%Y-%m-%d")

        date = local_time.date()
        time = local_time.time()
        month = local_time.month

        if month in (12, 1, 2):
            season = "Winter"
        elif month in (3, 4, 5):
            season = "Spring"
        elif month in (6, 7, 8):
            season = "Summer"
        else:
            season = "Autumn"

        return {"date": date, "time": time, "season": season}
    except Exception:
        return {"date": datetime.utcnow().date(), "time": datetime.utcnow().time(), "season": "Unknown"}


def get_DimWeatherEvent(data, source):
    try:
        if source == "OWM":
            weather = data["weather"][0]
            event_id = int(weather.get("id", 0))
            condition = weather.get("main", "Unknown")
            desc = weather.get("description", "N/A")
        else:
            day = data["days"][0]
            event_id = int(day.get("conditions", "").__hash__() % 10000)
            condition = day.get("conditions", "Unknown")
            desc = day.get("description", "N/A")

        # Determine severity
        severity = "Light" if "light" in desc.lower() else (
            "Severe" if "heavy" in desc.lower() or "extreme" in desc.lower() else "Moderate"
        )

        return {
            "event_id": event_id,
            "Weather_condition": condition,
            "description": desc,
            "severity": severity
        }
    except Exception:
        return {"event_id": 0, "Weather_condition": "Unknown", "description": "N/A", "severity": "Unknown"}


def get_DimLocation(data, city_name, source):
    try:
        if source == "OWM":
            country = data.get("sys", {}).get("country", "Unknown")
            lat = data.get("coord", {}).get("lat", 0.0)
            lon = data.get("coord", {}).get("lon", 0.0)
        else:
            location = data.get("resolvedAddress", city_name)
            lat = data.get("latitude", 0.0)
            lon = data.get("longitude", 0.0)
            country = location.split(",")[-1].strip()

        return {"city_name": city_name, "country": country, "lat": lat, "lon": lon}
    except Exception:
        return {"city_name": city_name, "country": "Unknown", "lat": 0.0, "lon": 0.0}


def get_FactWeatherObservation(data, source):
    try:
        if source == "OWM":
            main = data.get("main", {})
            wind = data.get("wind", {})
            clouds = data.get("clouds", {})
            return {
                "temp": main.get("temp", 0.0),
                "humidity": main.get("humidity", 0.0),
                "pressure": main.get("pressure", 0.0),
                "wind_speed": wind.get("speed", 0.0),
                "wind_direction": wind.get("deg", 0.0),
                "visibility": data.get("visibility", 0.0),
                "cloud_coverage": clouds.get("all", 0.0),
                "precipitation": data.get("rain", {}).get("1h", 0.0) + data.get("snow", {}).get("1h", 0.0),
                "model_id": 1
            }
        else:
            day = data["days"][0]
            return {
                "temp": day.get("temp", 0.0),
                "humidity": day.get("humidity", 0.0),
                "pressure": day.get("pressure", 0.0),
                "wind_speed": day.get("windspeed", 0.0),
                "wind_direction": day.get("winddir", 0.0),
                "visibility": day.get("visibility", 0.0),
                "cloud_coverage": day.get("cloudcover", 0.0),
                "precipitation": day.get("precip", 0.0),
                "model_id": 2
            }
    except Exception:
        return {k: 0.0 for k in ["temp", "humidity", "pressure", "wind_speed", "wind_direction", "visibility", "cloud_coverage", "precipitation"]}


# ================== Database Insert ==================
def insert_DimTime(data, conn, cursor):
    cursor.execute("""
        INSERT INTO DimTime(time_date, season, time_value)
        OUTPUT INSERTED.time_id VALUES (?, ?, ?)
    """, (data["date"], data["season"], data["time"]))
    TimeID = cursor.fetchone()[0]   
    conn.commit()
    return TimeID


def insert_DimWeatherEvent(data, conn, cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM DimWeatherEvent WHERE event_id = ?)
        INSERT INTO DimWeatherEvent(event_id, Weather_condition, description, severity)
        VALUES (?, ?, ?, ?)
    """, (data["event_id"], data["event_id"], data["Weather_condition"], data["description"], data["severity"]))
    conn.commit()
    return data["event_id"]


def insert_DimLocation(data, conn, cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM DimLocation WHERE city = ?)
        INSERT INTO DimLocation(city, country, latitude, longitude)
        VALUES (?, ?, ?, ?)
    """, (data["city_name"], data["city_name"], data["country"], data["lat"], data["lon"]))
    conn.commit()

    cursor.execute("SELECT location_id FROM DimLocation WHERE city = ?", data["city_name"])
    return cursor.fetchone()[0]


def insert_FactWeatherObservation(data, conn, cursor):
    cursor.execute("""
        INSERT INTO FactWeatherObservation(
            time_id, location_id, event_id,
            temperature, humidity, wind_speed, wind_direction,
            pressure, precipitation, visibility, cloud_coverage, model_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data["TimeID"], data["location_id"], data["event_id"],
        data["temp"], data["humidity"], data["wind_speed"],
        data["wind_direction"], data["pressure"], data["precipitation"],
        data["visibility"], data["cloud_coverage"], 
        data['model_id']
    ))
    conn.commit()


# ================== Main Task ==================
def get_data():
    API_KEY_OWM = "6f4f6fd9823133fe2823c206c116dd7f"
    API_KEY_VC = "T8RQBDHPFDJMV568LRKJJZNNG"

    cities = cities_Names()
    conn, cursor = connection()

    for city in cities:

        list= API_Request(city, API_KEY_OWM, API_KEY_VC)
        
        for data,source in list:
          time_data = get_DimTime(data, source)
          event_data = get_DimWeatherEvent(data, source)
          location_data = get_DimLocation(data, city, source)
          fact_data = get_FactWeatherObservation(data, source)

          TimeID = insert_DimTime(time_data, conn, cursor)
          event_id = insert_DimWeatherEvent(event_data, conn, cursor)
          location_id = insert_DimLocation(location_data, conn, cursor)

          fact_data.update({
            "TimeID": TimeID,
            "event_id": event_id,
            "location_id": location_id
                         })
          insert_FactWeatherObservation(fact_data, conn, cursor)

    conn.close()


# ================== Airflow DAG ==================
default_args = {
    "owner": "omar",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

dag = DAG(
    "weather_pipeline",
    description="Weather ETL using OpenWeatherMap & VisualCrossing APIs",
    schedule_interval="0 */3 * * *",
    default_args=default_args,
    catchup=False,
)

task1 = PythonOperator(
    task_id="get_data",
    python_callable=get_data,
    dag=dag
)
