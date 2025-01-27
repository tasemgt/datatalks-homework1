SQL CODE

Q1
```
docker run -it --entrypoint=bash python:3.12.8
pip --version

```

Q3
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

Q4

```

SELECT 
    DATE(lpep_pickup_datetime) AS pickup_day, 
    MAX(trip_distance) AS longest_trip_distance
FROM 
    green_taxi_data
GROUP BY 
    DATE(lpep_pickup_datetime)
ORDER BY 
    longest_trip_distance DESC
LIMIT 1;

```

Q5

```
SELECT 
    l."Zone" AS pickup_zone,
    SUM(t.total_amount) AS total_revenue
FROM 
    green_taxi_data t
JOIN 
    taxi_zone_lookup l
ON 
    t."PULocationID" = l."LocationID"
WHERE 
    DATE(t.lpep_pickup_datetime) = '2019-10-18'
GROUP BY 
    l."Zone"
HAVING 
    SUM(t.total_amount) > 13000
ORDER BY 
    total_revenue DESC;

```

Q6

```
SELECT 
    l_drop."Zone" AS dropoff_zone,
    MAX(t.tip_amount) AS largest_tip
FROM 
    green_taxi_data t
JOIN 
    taxi_zone_lookup l_pick ON t."PULocationID" = l_pick."LocationID"
JOIN 
    taxi_zone_lookup l_drop ON t."DOLocationID" = l_drop."LocationID"
WHERE 
    l_pick."Zone" = 'East Harlem North'
    AND DATE_PART('month', t.lpep_pickup_datetime) = 10
    AND DATE_PART('year', t.lpep_pickup_datetime) = 2019
GROUP BY 
    l_drop."Zone"
ORDER BY 
    largest_tip DESC
LIMIT 1;

```






