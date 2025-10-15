from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
import json
import requests
import pyodbc
from datetime import datetime, timedelta
import os
from airflow.utils.dates import days_ago


def connection():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=host.docker.internal,1433;"
        "DATABASE=WeatherDB;"
        "UID=docker_user;"
        "PWD=9512;"
        "TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return conn, cursor


def cities_Names():
    return [
        "Cairo", "Alexandria",
        "New York", "Los Angeles",
        "London", "Manchester",
        "Berlin", "Munich",
        "Paris", "Marseille",
        "Mumbai", "Delhi",
        "Tokyo", "Osaka",
        "Sao Paulo", "Rio de Janeiro",
        "Toronto", "Vancouver",
        "Sydney", "Melbourne"
    ]


def API_Request(city_name, API_KEY_OWM, API_KEY_VC):
    results = []

    # Visual Crossing
    url_vc = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city_name}?unitGroup=metric&key={API_KEY_VC}&contentType=json"
    try:
        res_vc = requests.get(url_vc, timeout=10)
        if res_vc.status_code == 200:
            results.append([res_vc.json(), "VC"])
    except Exception as e:
        print(f"⚠️ VC request error for {city_name}: {e}")

    # OpenWeather
    url_owm = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={API_KEY_OWM}&units=metric"
    try:
        res_owm = requests.get(url_owm, timeout=10)
        if res_owm.status_code == 200:
            results.append([res_owm.json(), "OWM"])
    except Exception as e:
        print(f"⚠️ OWM request error for {city_name}: {e}")

    if not results:
        print(f"⚠️ No data found for {city_name}")
    return results


def get_City(data, source):
    if source == "OWM":
        city_name = data.get("name", "Unknown")
        country = data.get("sys", {}).get("country", "Unknown")
        lat = data.get("coord", {}).get("lat", 0.0)
        lon = data.get("coord", {}).get("lon", 0.0)
    else:
        city_name = data.get("resolvedAddress", "Unknown")
        lat = data.get("latitude", 0.0)
        lon = data.get("longitude", 0.0)
        country = city_name.split(",")[-1].strip()
    return {"city_name": city_name, "country": country, "lat": lat, "lon": lon}


def get_WeatherCondition(data, source):
    if source == "OWM":
        weather_list = data.get("weather", [])
        if not weather_list:
            return {"ConditionID": 0, "Weather_condition": "Unknown", "description": "N/A"}

        weather = weather_list[0]
        ConditionID = int(weather.get("id", 0))
        Weather_condition = weather.get("main", "Unknown")
        description = weather.get("description", "N/A")
    else:
        days = data.get("days", [])
        if not days:
            return {"ConditionID": 0, "Weather_condition": "Unknown", "description": "N/A"}

        day = days[0]
        cond = day.get("conditions", "Unknown")
        ConditionID = abs(hash(cond)) % 10000
        Weather_condition = cond
        description = day.get("description", "N/A")

    return {"ConditionID": ConditionID, "Weather_condition": Weather_condition, "description": description}


def get_WeatherObservation(data, source):
    if source == "OWM":
        main = data.get("main", {})
        wind = data.get("wind", {})
        clouds = data.get("clouds", {})
        dt_utc = data.get("dt", int(datetime.utcnow().timestamp()))
        tz_offset = data.get("timezone", 0)
        local_time = datetime.utcfromtimestamp(dt_utc) + timedelta(seconds=tz_offset)

        return {
            "temp": main.get("temp", 0.0),
            "humidity": main.get("humidity", 0.0),
            "pressure": main.get("pressure", 0.0),
            "wind_speed": wind.get("speed", 0.0),
            "wind_direction": wind.get("deg", 0.0),
            "visibility": data.get("visibility", 0.0),
            "cloud_coverage": clouds.get("all", 0.0),
            "local_time": local_time,
        }

    else:
        days = data.get("days", [])
        if not days:
            return {
                "temp": 0.0,
                "humidity": 0.0,
                "pressure": 0.0,
                "wind_speed": 0.0,
                "wind_direction": 0.0,
                "visibility": 0.0,
                "cloud_coverage": 0.0,
                "local_time": datetime.utcnow(),
            }

        day = days[0]
        local_time = datetime.strptime(day.get("datetime", datetime.utcnow().strftime("%Y-%m-%d")), "%Y-%m-%d")

        return {
            "temp": day.get("temp", 0.0),
            "humidity": day.get("humidity", 0.0),
            "pressure": day.get("pressure", 0.0),
            "wind_speed": day.get("windspeed", 0.0),
            "wind_direction": day.get("winddir", 0.0),
            "visibility": day.get("visibility", 0.0),
            "cloud_coverage": day.get("cloudcover", 0.0),
            "local_time": local_time,
        }


def extract_data(data, source):
    City_data = get_City(data, source)
    WeatherCondition_data = get_WeatherCondition(data, source)
    Observation_data = get_WeatherObservation(data, source)
    return City_data, WeatherCondition_data, Observation_data


def insert_city(City_data, conn, cursor):
    cursor.execute(
        """IF NOT EXISTS (SELECT 1 FROM City WHERE City = ?)
           INSERT INTO City (City, Country, Latitude, Longitude)
           VALUES (?, ?, ?, ?)""",
        (City_data["city_name"], City_data["city_name"], City_data["country"], City_data["lat"], City_data["lon"]),
    )
    conn.commit()

    cursor.execute("SELECT CityID FROM City WHERE City = ?", City_data["city_name"])
    result = cursor.fetchone()
    return result[0] if result else None


def insert_WeatherCondition(WeatherCondition_data, conn, cursor):
    cursor.execute(
        """IF NOT EXISTS (SELECT 1 FROM WeatherCondition WHERE ConditionID = ?)
           INSERT INTO WeatherCondition (ConditionID, ConditionName, Description)
           VALUES (?, ?, ?)""",
        (   WeatherCondition_data["ConditionID"],
            WeatherCondition_data["ConditionID"],
            WeatherCondition_data["Weather_condition"],
            WeatherCondition_data["description"],
        ),
    )
    conn.commit()
    return WeatherCondition_data["ConditionID"]


def insert_WeatherObservation(Observation_data, conn, cursor):
    cursor.execute(
        """INSERT INTO WeatherObservation
           (CityID, ConditionID, temperature, humidity, wind_speed, wind_direction,
            pressure, visibility, cloud_coverage, Observation_Datetime, Model_num)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            Observation_data["CityID"],
            Observation_data["ConditionID"],
            Observation_data["temp"],
            Observation_data["humidity"],
            Observation_data["wind_speed"],
            Observation_data["wind_direction"],
            Observation_data["pressure"],
            Observation_data["visibility"],
            Observation_data["cloud_coverage"],
            Observation_data["local_time"],
            Observation_data["Model_num"],
        ),
    )
    conn.commit()


def get_data():
    API_KEY_OWM = "6f4f6fd9823133fe2823c206c116dd7f"
    API_KEY_VC = "T8RQBDHPFDJMV568LRKJJZNNG"

    cities = cities_Names()
    conn, cursor = connection()

    for city in cities:
        responses = API_Request(city, API_KEY_OWM, API_KEY_VC)
        if not responses:
            continue

        for data, source in responses:
            City_data, WeatherCondition_data, Observation_data = extract_data(data, source)

            CityID = insert_city(City_data, conn, cursor)
            ConditionID = insert_WeatherCondition(WeatherCondition_data, conn, cursor)

            Observation_data["CityID"] = CityID
            Observation_data["ConditionID"] = ConditionID
            Observation_data["Model_num"] = 1 if source == "OWM" else 2

            insert_WeatherObservation(Observation_data, conn, cursor)

    conn.close()


dag = DAG(
    "weather_pipeline",
    start_date=days_ago(1),
    schedule_interval="*/3 * * * *",  # كل 3 دقائق
    catchup=False,
    max_active_runs=1,
)

task1 = PythonOperator(
    task_id="get_data",
    python_callable=get_data,
    dag=dag,
)
