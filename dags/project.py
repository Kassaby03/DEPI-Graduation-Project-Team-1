from airflow import DAG
from airflow.operators.python import PythonOperator


from dotenv import load_dotenv
import json
import requests
import pyodbc
from datetime import datetime
import os

def fetch_weather():
    API_KEY = "6f4f6fd9823133fe2823c206c116dd7f"
    conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    # "SERVER=host.docker.internal,1433;"
    "SERVER=host.docker.internal;"
    "DATABASE=IOT;"
    "UID=docker_user;"
    "PWD=9512;"
    "TrustServerCertificate=yes;"
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

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

    for city_name in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={API_KEY}&units=metric"
        response = requests.get(url, timeout=10)
        if response.status_code != 200:
            print(f"Skipping {city_name}, API error: {response.status_code}")
            continue
        data = response.json()

        # Extract city info
        city_name_api = data.get("name")
        country = data.get("sys", {}).get("country")
        lat = data.get("coord", {}).get("lat")
        lon = data.get("coord", {}).get("lon")

        # Insert city if not exists
        cursor.execute("SELECT CityID FROM Cities WHERE CityName=? AND Country=?", city_name_api, country)
        row = cursor.fetchone()

        if row:
            city_id = row[0]
        else:
            cursor.execute(
                "INSERT INTO Cities (CityName, Country, Latitude, Longitude) OUTPUT INSERTED.CityID VALUES (?, ?, ?, ?)",
                city_name_api, country, lat, lon
            )
            city_id = cursor.fetchone()[0]
        
        conn.commit()

        # Extract weather info
        main = data.get("main", {})
        wind = data.get("wind", {})
        weather_list = data.get("weather", [{}])

        temp = main.get("temp")
        feels_like = main.get("feels_like")
        humidity = main.get("humidity")
        pressure = main.get("pressure")
        wind_speed = wind.get("speed", 0)
        wind_deg = wind.get("deg", 0)
        weather_main = weather_list[0].get("main")
        weather_desc = weather_list[0].get("description")
        now = datetime.now()

        # Insert weather measurement
        cursor.execute("""
            INSERT INTO WeatherMeasurements
            (CityID, Temperature, FeelsLike, Humidity, Pressure, WindSpeed, WindDirection, WeatherMain, WeatherDescription, DateTimeRecorded)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, city_id, temp, feels_like, humidity, pressure, wind_speed, wind_deg, weather_main, weather_desc, now)
        conn.commit()

    #     print(f"Inserted data for {city_name_api}, {country}")

    conn.close()
    # print("All cities processed successfully!")


dag = DAG(
    'weather_pipeline',
    start_date=datetime.now(),
    # schedule_interval='@hourly'
    schedule_interval='*/3 * * * *',
    catchup=False
)

task1 = PythonOperator(
    task_id='fetch_weather',
    python_callable=fetch_weather,
    dag=dag
)


