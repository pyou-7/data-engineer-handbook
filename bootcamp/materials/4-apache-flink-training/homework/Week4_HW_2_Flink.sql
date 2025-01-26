--Creating PostgreSQL sink table
CREATE TABLE sessionized_events (
    ip VARCHAR,
    host VARCHAR,
    session_start TIMESTAMP(3),
    session_end TIMESTAMP(3),
    num_events BIGINT  -- Ensure this matches the data type returned by count
);

-- DROP TABLE IF EXISTS sessionized_events;

--Checking the data
SELECT * FROM sessionized_events;

-- Q1: Calculate the average number of web events per session for users on Tech Creator
-- The query filters sessions where the host includes 'techcreator.io' and calculates the average number of events per session
SELECT
    AVG(num_events) AS avg_events_per_session
FROM sessionized_events
WHERE host LIKE '%techcreator.io';

-- Q2: Compare the average number of web events per session across specific hosts
-- The query focuses on sessions from three specific hosts and calculates the average number of events per session.
SELECT
    host,
    AVG(num_events) AS avg_events_per_session
FROM sessionized_events
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY avg_events_per_session DESC;
