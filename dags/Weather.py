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
    "DATABASE=Weather;"
    "UID=docker_user;"
    "PWD=9512;"
    "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    return conn, cursor



def cities_Names():
    cities = [
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
    return cities


def API_Request(city_name, API_KEY):
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={API_KEY}&units=metric"
    response = requests.get(url, timeout=10)
    if response.status_code != 200:
        print(f"Skipping {city_name}, API error: {response.status_code}")
        return
    data = response.json()
    return data


def get_DimTime(data):
    dt_utc = data["dt"]
    timezone_offset = data["timezone"]

    utc_time = datetime.utcfromtimestamp(dt_utc)

    local_time = utc_time + timedelta(seconds=timezone_offset)

    date = local_time.date()
    time = local_time.time()

    month = local_time.month
    # season = ""
    if month in (12, 1, 2):
        season = "Winter"
    elif month in (3, 4, 5):
        season = "Spring"
    elif month in (6, 7, 8):
        season = "Summer"
    else:
        season = "Autumn"
        
    return {"date": date,
            "time": time,
            "season": season}


def get_DimWeatherEvent(data):
    weather = data["weather"][0]   # take the first weather condition
    event_id = weather["id"]
    Weather_condition = weather["main"]
    description = weather["description"]

    # Map severity based on description
    if "light" in description:
        severity = "Light"
    elif "heavy" in description or "extreme" in description:
        severity = "Severe"
    else:
        severity = "Moderate"

    return {"event_id": event_id,
            "Weather_condition": Weather_condition,
            "description": description,
            "severity": severity}


def get_DimLocation(data):
    city_name = data.get("name")
    country = data.get("sys", {}).get("country")
    lat = data.get("coord", {}).get("lat")
    lon = data.get("coord", {}).get("lon")
    return {"city_name": city_name,
            "country": country,
            "lat": lat,
            "lon": lon}


def get_FactWeatherObservation(data):
    temp = data.get("main",{}).get("temp")
    humidity = data.get("main",{}).get("humidity")
    pressure = data.get("main",{}).get("pressure")
    wind_speed = data.get("wind", {}).get("speed", 0.0)
    wind_direction = data.get("wind", {}).get("deg")
    cloud_coverage = data["clouds"].get("all", 0.0)
    visibility = data.get("visibility", 0.0)

    rain_1h = data.get("rain", {}).get("1h", 0.0)
    snow_1h = data.get("snow", {}).get("1h", 0.0)
    precipitation = rain_1h + snow_1h

    return {
        "temp": temp,
        "humidity": humidity,
        "pressure": pressure,
        "wind_speed": wind_speed,
        "wind_direction": wind_direction,
        "visibility": visibility,
        "cloud_coverage": cloud_coverage,
        "precipitation": precipitation,
    }


def extract_data(data):
    DimTime_data = get_DimTime(data)
    DimWeatherEvent_data = get_DimWeatherEvent(data)
    DimLocation_data = get_DimLocation(data)
    FactWeatherObservation_data = get_FactWeatherObservation(data)
    
    return DimTime_data, DimWeatherEvent_data, DimLocation_data, FactWeatherObservation_data


def insert_DimTime(DimTime_data, conn, cursor):
    cursor.execute("""insert into DimTime(time_date, season, time_value)
                        OUTPUT INSERTED.time_id values(?,?,?)""",
                        DimTime_data['date'], DimTime_data['season'], DimTime_data['time'])
    
    TimeID = cursor.fetchone()[0]
    conn.commit()
    return TimeID


def insert_DimWeatherEvent(DimWeatherEvent_data, conn, cursor):
    cursor.execute("""IF NOT EXISTS (SELECT 1 FROM DimWeatherEvent WHERE event_id = ?)
                        insert into DimWeatherEvent(event_id, Weather_condition, description, severity)
                        values(?,?,?,?)""",
                        (DimWeatherEvent_data['event_id'], DimWeatherEvent_data['event_id'], DimWeatherEvent_data['Weather_condition'], DimWeatherEvent_data['description'], DimWeatherEvent_data['severity']))
    
    conn.commit()
    return DimWeatherEvent_data['event_id']


def insert_DimLocation(DimLocation_data, conn, cursor):
    cursor.execute("""if not exists (select 1 from DimLocation where city = ?)
                        insert into DimLocation(city, country, latitude, longitude)
                        values(?,?,?,?)""",
                        (DimLocation_data['city_name'], DimLocation_data['city_name'], DimLocation_data['country'],
                        DimLocation_data['lat'], DimLocation_data['lon']))
    conn.commit()

    cursor.execute("SELECT location_id FROM DimLocation WHERE city = ?", DimLocation_data['city_name'])
    location_id = cursor.fetchone()[0]
    return location_id


def insert_FactWeatherObservation(Fact_data, conn, cursor):
    cursor.execute("""insert into FactWeatherObservation(time_id, location_id, event_id,
                        temperature, humidity, wind_speed, wind_direction,
                        pressure, precipitation, visibility, cloud_coverage)
                        values(?,?,?,?,?,?,?,?,?,?,?)""",
                        
                        (Fact_data['TimeID'], Fact_data['location_id'], Fact_data['event_id'],
                        Fact_data['temp'], Fact_data['humidity'], Fact_data['wind_speed'],
                        Fact_data['wind_direction'], Fact_data['pressure'], Fact_data['precipitation'],
                        Fact_data['visibility'], Fact_data['cloud_coverage']))
    conn.commit()

def get_data():
    API_KEY = "6f4f6fd9823133fe2823c206c116dd7f"
    cities = cities_Names()
    conn, cursor = connection()
    for city in cities:
        data = API_Request(city, API_KEY)
        DimTime_data, DimWeatherEvent_data, DimLocation_data, FactWeatherObservation_data = extract_data(data)
        
        TimeID = insert_DimTime(DimTime_data, conn, cursor)
        event_id = insert_DimWeatherEvent(DimWeatherEvent_data, conn, cursor)
        location_id = insert_DimLocation(DimLocation_data, conn, cursor)

        FactWeatherObservation_data['TimeID'] = TimeID
        FactWeatherObservation_data['event_id'] = event_id
        FactWeatherObservation_data['location_id'] = location_id
        
        insert_FactWeatherObservation(FactWeatherObservation_data, conn, cursor)

    conn.close()



dag = DAG(
    'weather_pipeline',
    start_date=days_ago(1),
    # schedule_interval='@hourly'
    schedule_interval='*/3 * * * *',
    catchup=False,
    max_active_runs=1
)

task1 = PythonOperator(
    task_id='get_data',
    python_callable=get_data,
    dag=dag
)





