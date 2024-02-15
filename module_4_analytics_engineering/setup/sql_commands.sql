/*
Data processed in the web_to_gcs.py file had different data types. I want to go in an define all of the dtypes for the load, 
but the team shared this SQL flow to ingest the data directly from public datasets. Going to use that for speed and consistency.
*/

CREATE SCHEMA IF NOT EXISTS `dezc-brett.ny_taxi`;

--load in not from buckets, but from public datasets

/*
GREEN TAXI DATA
*/

CREATE TABLE  `dezc-brett.ny_taxi.green_tripdata` as
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2019`; 

insert into  `dezc-brett.ny_taxi.green_tripdata`
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_green_trips_2020` ;

  -- Fixes green table schema
ALTER TABLE `dezc-brett.ny_taxi.green_tripdata`
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `dezc-brett.ny_taxi.green_tripdata`
  RENAME COLUMN pickup_datetime TO lpep_pickup_datetime;
ALTER TABLE `dezc-brett.ny_taxi.green_tripdata`
  RENAME COLUMN dropoff_datetime TO lpep_dropoff_datetime;
ALTER TABLE `dezc-brett.ny_taxi.green_tripdata`
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `dezc-brett.ny_taxi.green_tripdata`
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `dezc-brett.ny_taxi.green_tripdata`
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `dezc-brett.ny_taxi.green_tripdata`
  RENAME COLUMN dropoff_location_id TO DOLocationID;


/*
YELLOW TAXI DATA
*/

CREATE TABLE  `dezc-brett.ny_taxi.yellow_tripdata` as
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2019`;

insert into  `dezc-brett.ny_taxi.yellow_tripdata` 
SELECT * FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2020`; 

  -- Fixes yellow table schema
ALTER TABLE `dezc-brett.ny_taxi.yellow_tripdata`
  RENAME COLUMN vendor_id TO VendorID;
ALTER TABLE `dezc-brett.ny_taxi.yellow_tripdata`
  RENAME COLUMN pickup_datetime TO tpep_pickup_datetime;
ALTER TABLE `dezc-brett.ny_taxi.yellow_tripdata`
  RENAME COLUMN dropoff_datetime TO tpep_dropoff_datetime;
ALTER TABLE `dezc-brett.ny_taxi.yellow_tripdata`
  RENAME COLUMN rate_code TO RatecodeID;
ALTER TABLE `dezc-brett.ny_taxi.yellow_tripdata`
  RENAME COLUMN imp_surcharge TO improvement_surcharge;
ALTER TABLE `dezc-brett.ny_taxi.yellow_tripdata`
  RENAME COLUMN pickup_location_id TO PULocationID;
ALTER TABLE `dezc-brett.ny_taxi.yellow_tripdata`
  RENAME COLUMN dropoff_location_id TO DOLocationID;