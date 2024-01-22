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
