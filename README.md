SQL CODE

```
SELECT COUNT(*) FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
  AND trip_distance > 1;

SELECT COUNT(*) FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
  AND trip_distance > 1 AND trip_distance <= 3;


SELECT COUNT(*) FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
  AND trip_distance > 3 AND trip_distance <= 7;



SELECT COUNT(*) FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
  AND trip_distance > 7 AND trip_distance <= 10;


SELECT COUNT(*) FROM green_taxi_data
WHERE lpep_pickup_datetime >= '2019-10-01' 
  AND lpep_pickup_datetime < '2019-11-01'
  AND trip_distance > 10

```
