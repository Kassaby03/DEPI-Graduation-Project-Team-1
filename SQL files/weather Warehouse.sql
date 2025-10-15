drop table DimTime
drop table DimWeatherEvent
drop table FactWeatherObservation
drop table DimLocation

CREATE TABLE DimTime (
    time_id INT identity(1,1) PRIMARY KEY,
    time_date DATE NOT NULL,
    season VARCHAR(20),
    time_value TIME NOT NULL
)
select * from DimTime

--CREATE TABLE DimForecastModel (
--    model_id INT PRIMARY KEY,
 --   model_name VARCHAR(100),
--    confidence_level DECIMAL(5, 2)
--)
CREATE TABLE DimWeatherEvent (
    event_id INT PRIMARY KEY,
    Weather_condition VARCHAR(50),
    description VARCHAR(50),
    severity varchar(50),
)
select * from DimWeatherEvent

drop table DimWeatherEvent

CREATE TABLE DimLocation (
    location_id INT identity (1,1) PRIMARY KEY,
    country VARCHAR(100),
    city VARCHAR(100),
    latitude DECIMAL(8, 6),
    longitude DECIMAL(9, 6),
    --elevation DECIMAL(6, 2)
)
select * from DimLocation

CREATE TABLE FactWeatherObservation (
    observation_id INT identity(1,1) PRIMARY KEY,
    time_id INT,
    location_id INT,
    event_id INT,
    --model_id INT,

    temperature DECIMAL(6, 2),
    humidity DECIMAL(5, 2),
    wind_speed DECIMAL(6, 2),
    wind_direction DECIMAL(6, 2),
    pressure DECIMAL(8, 2),
    precipitation DECIMAL(6, 2),
    visibility DECIMAL(8, 2),
    cloud_coverage DECIMAL(5, 2),
    
    -- Foreign Keys
    FOREIGN KEY (time_id) REFERENCES DimTime(time_id),
    FOREIGN KEY (location_id) REFERENCES DimLocation(location_id),
    FOREIGN KEY (event_id) REFERENCES DimWeatherEvent(event_id)
    -- FOREIGN KEY (model_id) REFERENCES DimForecastModel(model_id)
)

drop table FactWeatherObservation

delete from FactWeatherObservation

select * from FactWeatherObservation
ALTER TABLE DimWeatherEvent 
ALTER COLUMN description NVARCHAR(MAX);

USE project;
GO

ALTER USER docker_user WITH LOGIN = docker_user;
GO

CREATE TABLE DimForecastModel (
    model_id INT IDENTITY(1,1) PRIMARY KEY,
    model_name VARCHAR(100) NOT NULL,
    api_url VARCHAR(255),
    description VARCHAR(255)
);
select * from DimForecastModel
INSERT INTO DimForecastModel (model_name, api_url, description)
VALUES 
('OpenWeather', 'https://api.openweathermap.org/data/2.5/weather', 'Global weather API providing real-time and forecast data'),
('VisualCrossing', 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline', 'Detailed weather API providing historical and forecast data');

ALTER TABLE FactWeatherObservation
ADD model_id INT;

ALTER TABLE FactWeatherObservation
ADD CONSTRAINT FK_FactWeatherObservation_Model
FOREIGN KEY (model_id) REFERENCES DimForecastModel(model_id);

delete from FactWeatherObservation
where time_id is null


BACKUP DATABASE WeatherDataDB
TO DISK = 'D:\GP DEPI\project.bak'
WITH FORMAT,
    NAME = 'Full Backup of WeatherDataDW',
    SKIP,
    STATS = 10;
