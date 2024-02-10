CREATE SCHEMA IF NOT EXISTS `dezc-mage-brett.ny_taxi`;


--create the external data from GCS bucket
CREATE OR REPLACE EXTERNAL TABLE `dezc-mage-brett.ny_taxi.taxi_data_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxi_data_2022_brettly/nyc_taxi_data_2022/*.parquet']
);

--How many records
SELECT
  COUNT(1)
FROM `dezc-mage-brett.ny_taxi.taxi_data_external`;


--materialize table
CREATE OR REPLACE TABLE `dezc-mage-brett.ny_taxi.taxi_data_materialized`
AS SELECT * FROM `dezc-mage-brett.ny_taxi.taxi_data_external`;

--validate number of records
SELECT
  COUNT(1)
FROM `dezc-mage-brett.ny_taxi.taxi_data_materialized`;


--how many records with $0 fare amount
SELECT
  COUNT(1)
FROM `dezc-mage-brett.ny_taxi.taxi_data_materialized`
WHERE 1=1
  AND fare_amount = 0;


--partition by pickup date, cluster by pickup location id
CREATE OR REPLACE TABLE `dezc-mage-brett.ny_taxi.taxi_data_partitioned`
PARTITION BY DATE(lpep_pickup_datetime)  -- Partitioning by day
CLUSTER BY PUlocationID AS (
  SELECT * FROM `dezc-mage-brett.ny_taxi.taxi_data_materialized`
);


--Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive). With materialized and partitioned
--materialized = 12.82MB
SELECT DISTINCT
  PULocationID
FROM `dezc-mage-brett.ny_taxi.taxi_data_materialized`
WHERE 1=1
  AND lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';

--paritioned = 1.12MB
SELECT DISTINCT
  PULocationID
FROM `dezc-mage-brett.ny_taxi.taxi_data_partitioned`
WHERE 1=1
  AND lpep_pickup_datetime BETWEEN '2022-06-01' AND '2022-06-30';


--Write a `SELECT count(*)` query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
SELECT count(*) FROM `dezc-mage-brett.ny_taxi.taxi_data_materialized`;

--estimates 0 byes. I belive this is because it pulls the metadata to get this, versus using up compute resources.