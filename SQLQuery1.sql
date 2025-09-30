create TABLE Cities (
    CityID INT IDENTITY(1,1) PRIMARY KEY,
    CityName NVARCHAR(100),
    Country NVARCHAR(50),
    Latitude FLOAT,
    Longitude FLOAT
);


CREATE TABLE WeatherMeasurements (
    MeasurementID INT IDENTITY(1,1) PRIMARY KEY,
    CityID INT FOREIGN KEY REFERENCES Cities(CityID),
    Temperature FLOAT,
    FeelsLike FLOAT,
    Humidity INT,
    Pressure INT,
    WindSpeed FLOAT,
    WindDirection INT,
    WeatherMain NVARCHAR(50),
    WeatherDescription NVARCHAR(100),
    DateTimeRecorded DATETIME
);


select *
from Cities

select *
from WeatherMeasurements
