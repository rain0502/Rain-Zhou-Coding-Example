-- Sample 1
CREATE TABLE scooter(
    scooter_id INT NOT NULL,
    status ENUM('online', 'offline', 'lost/stolen') NOT NULL DEFAULT 'offline',
    PRIMARY KEY (scooter_id),
    FOREIGN KEY (scooter_id) REFERENCES trip(scooter_id)
);
CREATE TABLE customer(
    user_id INT NOT NULL,
    ccnum INT(16),
    expdate DATE,
    email VARCHAR(100),
    PRIMARY KEY (user_id),
    FOREIGN KEY (user_id) REFERENCES trip(user_id)
);
CREATE TABLE trip(
    trip_id BIGINT NOT NULL PRIMARY KEY,
    user_id INT,
    scooter_id INT,
    start_time DATETIME NOT NULL,
    end_time DATETIME,
    pickup_lat DECIMAL NOT NULL ,
    pickup_long DECIMAL NOT NULL,
    dropoff_lat DECIMAL,
    dropoff_long DECIMAL
);
ALTER TABLE customer
ADD UNIQUE (email);

use mgmtmsa402;
SHOW CREATE TABLE lax_pax;
SELECT
    MIN(report_month) AS earliest_record,
    MAX(report_month) AS latest_record
FROM lax_pax;
-- The earliest record is 2006-01-01 and the latest record is 2023-08-01.
SELECT
    COUNT(report_month) AS num_rows,
    terminal,
    movement,
    flight
FROM lax_pax
GROUP BY terminal, movement, flight;
SELECT
    SUM(throughput) AS total_pax,
    terminal,
    movement
FROM lax_pax
GROUP BY terminal, movement;
SELECT
    SUM(throughput) AS total_pax,
    terminal
FROM lax_pax
GROUP BY terminal
ORDER BY total_pax DESC
LIMIT 1;
use mgmtmsa402
SELECT
    terminal,
    YEAR(report_month) AS year,
    AVG(throughput) AS average
FROM lax_pax
WHERE movement = 'Departure'
GROUP BY terminal, year
HAVING SUM(throughput) > 1000000;
use mgmtmsa402;
SELECT
    terminal,
    YEAR(report_month) AS year,
    AVG(throughput) AS average
FROM lax_pax
WHERE
    movement = 'Arrival'
    AND terminal = 'TBIT'
    AND ((report_month BETWEEN '2016-01-01' AND '2019-12-01') OR report_month >= '2022-01-01')
GROUP BY year;

-- Sample 2
select *
from sf_trip_start
limit 10;
select *
from sf_trip_end
limit 10;
select *
from sf_user
limit 10;
-- Part 1 (a)
select
s.id as trip_id,
ceiling(minute(timediff(e.date, s.date))) as trip_length
from sf_trip_start s
left join sf_trip_end e
on s.id = e.id
order by trip_id;
-- Part 1 (b)
select
count(s.id) as stolen_bikes
from sf_trip_start s
left join sf_trip_end e
on s.id = e.id
where e.date is NULL;
-- Part 1 (c)
select
s.id as trip_id,
coalesce(3.49 + 0.3 * ceiling(minute(timediff(e.date, s.date))), 1000) as trip_charge
from sf_trip_start s
left join sf_trip_end e
on s.id = e.id
order by trip_id;
-- Part 1 (d)
select
aggregate.trip_id,
case
    when u.user_type = 'Subscriber' then coalesce(0.2 * trip_length, 1000)
    when u.user_type = 'Customer' then coalesce(3.49 + 0.3 * trip_length, 1000)
end as trip_charge
from (
select
s.id as trip_id,
e.date,
ceiling(minute(timediff(e.date, s.date))) as trip_length
from sf_trip_start s
left join sf_trip_end e
on s.id = e.id
) aggregate
left join sf_user u
on u.trip_id = aggregate.trip_id
order by aggregate.trip_id;
-- Part 1 (f): when we use the ON clause to filter the date, the results are completely different from what they are using the WHERE clause
-- (both trip_id and trip_charge). This is because the ON clause is executed before the join happens, which means records will be eliminated if the conditions are not met.
-- In this case, all the records that are not within the date range are not included in the result table, causing NULL values and trip charges being $1,000 after calculation.
-- On the other hand, if we use the WHERE clause, which is executed after the join, we will be able to include all records and only filter for the date range for calculation.
-- Therefore, when we are choosing between the ON and WHERE clauses, we should be aware that only conditions needed for the join should be included within the ON clause and the WHERE clause is generally used for filtering records, in order to avoid accidentally deleting records.
select
s.id as trip_id,
case when e.date is NULL then 1000
else 3.49 + 0.3 * ceiling(minute(timediff(e.date, s.date)))
end as trip_charge
from sf_trip_start s
left join sf_trip_end e
on (date(s.date) between '2018-03-01' and '2018-03-31') and s.id = e.id
order by trip_id;
select
s.id as trip_id,
case when e.date is NULL then 1000
else 3.49 + 0.3 * ceiling(minute(timediff(e.date, s.date)))
end as trip_charge
from sf_trip_start s
left join sf_trip_end e
on s.id = e.id
where date(s.date) between '2018-03-01' and '2018-03-31'
order by trip_id;
-- Part 2 (b)
select *
from sw_aircraft;
select *
from sw_airtran_aircraft
limit 10;
select *
from sw_flight
limit 10;
select *
from sw_airport
limit 10;
select
    a.type,
    round((count(distinct concat(f.origin, ' ', f.dest)))/(select count(distinct concat(origin, ' ', dest)) from sw_flight) * 100, 2) as percentage
from sw_flight f
left join sw_aircraft a
on f.tail = a.tail
group by type;
-- Part 2 (d)
SELECT DISTINCT
    flight_num
FROM sw_flight
WHERE tail NOT IN (
    SELECT tail
    FROM sw_airtran_aircraft
);
-- Part 2 (e)
-- 1st Alternative Way
SELECT DISTINCT
    flight_num
FROM sw_flight f
LEFT JOIN sw_airtran_aircraft t
ON f.tail = t.tail
WHERE t.tail is null;
-- 2nd Alternative Way
SELECT DISTINCT
    flight_num
FROM sw_flight f
WHERE NOT EXISTS (
SELECT 1
FROM sw_airtran_aircraft t
WHERE f.tail = t.tail);
-- Part 2 (f)
select
L.origin as origin,
R.origin as layover,
R.dest as final_dest,
L.flight_num as first_flight,
R.flight_num as second_flight,
L.departure as departure_from_lax,
R.arrival as arrival_in_sea
from sw_flight L
join sw_flight R
on L.dest = R.origin
where L.origin = 'LAX'
    and R.dest = 'SEA'
    and R.date = '2023-10-18'
    and L.date = '2023-10-18'
    and timediff(R.departure, L.arrival) between '01:00:00' and '03:00:00';
-- Part 2 (g)
WITH RECURSIVE FlightCTE AS (
  -- Base Case
  SELECT tail, dest, departure, 1 AS flight_rank
  FROM sw_flight
  WHERE departure = (SELECT MIN(departure) FROM sw_flight)

  UNION ALL

  -- Recursive Case
  SELECT sf.tail, sf.dest, sf.departure, f.flight_rank + 1
  FROM FlightCTE f
  JOIN sw_flight sf ON f.dest = sf.origin
  where f.flight_rank <= 4
)
SELECT tail, dest, departure
FROM FlightCTE
WHERE flight_rank = 4;

-- Sample 3
-- 1
use mgmtmsa402;
select *
from hw3_heartrate
order by user_id
limit 2;
use mgmtmsa402;
select *
from hw3_step
limit 2;
-- 1.1
select user_id, min(tstamp) from
    (select
    user_id,
    tstamp,
    sum(steps) over (partition by user_id, date(tstamp) order by tstamp) as total_steps
from hw3_step) sub
where sub.total_steps >= 10000
group by user_id, date(tstamp);
-- 1.2
select
    user_id,
    tstamp,
    heartrate as original_reading,
    avg(heartrate) over (partition by user_id order by tstamp rows between 4 preceding and 4 following) as smoothed_reading
from hw3_heartrate;
-- 1.3
select
    hour(tstamp) as hour,
    sum(steps)/count(distinct user_id) as AVG
from hw3_step
where date(tstamp) = '2016-04-16'
group by hour(tstamp);
-- 2
select address
from hw3_airbnb
limit 100;
-- 2.1
select
    listing_url,
    name
from hw3_airbnb
where json_extract(address, '$.market') = 'Oahu'
and 'Wifi' member of (amenities)
and (property_type = 'Condominium' or property_type = 'Apartment' or property_type = 'House')
and bed_type = 'Real Bed'
and maximum_nights >= 7 and minimum_nights <= 7
and lower(summary) like '%ocean view%';
-- There are 19 listings returned after filtering.