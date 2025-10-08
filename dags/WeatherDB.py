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
    "DATABASE=WeatherDataDB;"
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


def get_City(data):
    city_name = data.get("name")
    country = data.get("sys", {}).get("country")
    lat = data.get("coord", {}).get("lat")
    lon = data.get("coord", {}).get("lon")
    return {"city_name": city_name,
            "country": country,
            "lat": lat,
            "lon": lon}


def get_WeatherCondition(data):
    weather = data["weather"][0]   # take the first weather condition
    ConditionID = weather["id"]
    Weather_condition = weather["main"]
    description = weather["description"]

    return {"ConditionID": ConditionID,
            "Weather_condition": Weather_condition,
            "description": description}


def get_WeatherObservation(data):
    temp = data.get("main",{}).get("temp")
    humidity = data.get("main",{}).get("humidity")
    pressure = data.get("main",{}).get("pressure")
    wind_speed = data.get("wind", {}).get("speed", 0.0)
    wind_direction = data.get("wind", {}).get("deg")
    cloud_coverage = data["clouds"].get("all", 0.0)
    visibility = data.get("visibility", 0.0)

    dt_utc = data["dt"]
    timezone_offset = data["timezone"]
    utc_time = datetime.utcfromtimestamp(dt_utc)
    local_time = utc_time + timedelta(seconds=timezone_offset)
        
    return {"temp": temp,
            "humidity": humidity,
            "pressure": pressure,
            "wind_speed": wind_speed,
            "wind_direction": wind_direction,
            "visibility": visibility,
            "cloud_coverage": cloud_coverage,
            "local_time": local_time}


def extract_data(data):
    City_data = get_City(data)
    WeatherCondition_data = get_WeatherCondition(data)
    Observation_data = get_WeatherObservation(data)
    
    return City_data, WeatherCondition_data, Observation_data


def insert_city(City_data, conn, cursor):
    cursor.execute(""" if not exists (select 1 from City where city = ?)
                    insert into City(city, country, latitude, longitude)
                    values(?,?,?,?)""",
                    (City_data['city_name'], City_data['city_name'], City_data['country'],
                        City_data['lat'], City_data['lon']))
    conn.commit()

    cursor.execute("select CityID from City where City = ?", City_data['city_name'])
    CityID = cursor.fetchone()[0]
    return CityID



def insert_WeatherCondition(WeatherCondition_data, conn, cursor):
    cursor.execute("""IF NOT EXISTS (SELECT 1 FROM WeatherCondition WHERE ConditionID = ?)
                        insert into WeatherCondition(ConditionID, ConditionName, Description)
                        values(?,?,?)""",
                        (WeatherCondition_data['ConditionID'], WeatherCondition_data['ConditionID'],
                        WeatherCondition_data['Weather_condition'], WeatherCondition_data['description']))
    
    conn.commit()
    return WeatherCondition_data['ConditionID']


def insert_WeatherObservation(Observation_data, conn, cursor):
    cursor.execute("""insert into WeatherObservation(CityID, ConditionID,
                        temperature, humidity, wind_speed, wind_direction,
                        pressure, visibility, cloud_coverage, Observation_Datetime)
                        values(?,?,?,?,?,?,?,?,?,?)""",
                        
                        (Observation_data['CityID'], Observation_data['ConditionID'],
                        Observation_data['temp'], Observation_data['humidity'], Observation_data['wind_speed'],
                        Observation_data['wind_direction'], Observation_data['pressure'],
                        Observation_data['visibility'], Observation_data['cloud_coverage'],Observation_data['local_time']))
    conn.commit()


def get_data():
    API_KEY = "6f4f6fd9823133fe2823c206c116dd7f"
    cities = cities_Names()
    conn, cursor = connection()
    for city in cities:
        data = API_Request(city, API_KEY)
        City_data, WeatherCondition_data, Observation_data = extract_data(data)
        
        CityID = insert_city(City_data, conn, cursor)
        ConditionID = insert_WeatherCondition(WeatherCondition_data, conn, cursor)

        Observation_data['CityID'] = CityID
        Observation_data['ConditionID'] = ConditionID
        
        insert_WeatherObservation(Observation_data, conn, cursor)

    conn.close()

dag = DAG(
    'weather_pipelinee',
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


