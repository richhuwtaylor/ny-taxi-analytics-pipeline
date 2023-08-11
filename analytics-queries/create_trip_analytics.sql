-- Create a dedicated trip_analytics table for Looker Studio by joining trip_dim on rate_code_dim and payment_type_dim.

CREATE OR REPLACE TABLE `ny-taxi-project-395312.ny_taxi_project.trip_analytics` AS (
SELECT
  f.vendor_id,
  f.trip_distance,
  f.passenger_count,
  f.tpep_pickup_datetime,
  f.tpep_dropoff_datetime,
  f.total_amount,
  f.tolls_amount,
  f.tip_amount,
  f.rate_code_id,
  d.rate_code_name,
  f.payment_type_id,
  p.payment_type_name,
  f.pu_location_id,
  f.do_location_id
FROM `ny-taxi-project-395312.ny_taxi_project.trip_fact` f
INNER JOIN `ny-taxi-project-395312.ny_taxi_project.rate_code_dim` d
ON f.rate_code_id = d.rate_code_id
INNER JOIN `ny-taxi-project-395312.ny_taxi_project.payment_type_dim` p
ON f.payment_type_id = p.payment_type_id);