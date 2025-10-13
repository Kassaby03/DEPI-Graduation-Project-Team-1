create table City(
	CityID int identity(1,1) primary key,
	country VARCHAR(50),
    city VARCHAR(50),
	latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
);

 create database WeatherDataDB

create table WeatherCondition (
    ConditionID INT PRIMARY KEY,
    ConditionName VARCHAR(50),
    Description varchar(100),
);
drop table WeatherCondition
create table WeatherObservation (
    WeatherID INT IDENTITY(1,1) PRIMARY KEY,
    CityID int foreign key references City(CityID),
    ConditionID int foreign key references WeatherCondition(ConditionID),
	temperature DECIMAL(5, 2),
    humidity DECIMAL(5, 2),
    wind_speed DECIMAL(6, 2),
    wind_direction DECIMAL(6, 2),
    pressure DECIMAL(7, 2),
    visibility DECIMAL(7, 2),
    cloud_coverage DECIMAL(5, 2),
    Observation_Datetime DATETIME2 NOT NULL
);

select * from City
select * from WeatherCondition
select * from WeatherObservation

ALTER AUTHORIZATION ON DATABASE::WeatherDataDB TO sa;

USE WeatherDataDB;
ALTER ROLE db_owner ADD MEMBER [pro];
SELECT SUSER_NAME() AS CurrentLogin;


USE master;
GO
SELECT name, state_desc, suser_sname(owner_sid) AS owner
FROM sys.databases;


USE master;
GO
ALTER AUTHORIZATION ON DATABASE::WeatherDataDB TO [kassaby\omark];
GO

SELECT name, suser_sname(owner_sid) AS owner
FROM sys.databases
WHERE name = 'WeatherDataDB';

ALTER ROLE db_owner ADD MEMBER pro;

USE WeatherDataDB;
GO
SELECT name, type_desc 
FROM sys.database_principals 
WHERE name = 'pro';

USE WeatherDataDB;
GO
CREATE USER pro FOR LOGIN pro;
ALTER ROLE db_owner ADD MEMBER pro;

ALTER TABLE WeatherObservation ADD column Model_num Smallint;

ALTER TABLE WeatherObservation 
ADD Model_num SMALLINT not null Default(1);