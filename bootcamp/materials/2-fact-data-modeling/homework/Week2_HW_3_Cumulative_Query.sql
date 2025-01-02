INSERT INTO user_devices_cumulated
WITH yesterday AS (
    SELECT *
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-30')
),
today AS (
    SELECT
        e.user_id,
        d.browser_type,
        DATE_TRUNC('day', CAST(e.event_time AS TIMESTAMP)) AS today_date,
        COUNT(1) AS num_events 
    FROM events e
    JOIN devices d
    ON e.device_id = d.device_id
    WHERE DATE_TRUNC('day', CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
    AND e.user_id IS NOT NULL
    GROUP BY e.user_id, d.browser_type, DATE_TRUNC('day', CAST(e.event_time AS TIMESTAMP))
)
SELECT
    COALESCE(t.user_id, CAST(y.user_id AS NUMERIC)) AS user_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    COALESCE(y.device_activity_datelist, ARRAY[]::DATE[])
    || CASE WHEN
                t.user_id IS NOT NULL
                THEN ARRAY[t.today_date]
                ELSE ARRAY[]::DATE[] 
                END AS device_activity_datelist,
    COALESCE(t.today_date, y.date + INTERVAL '1 day') AS date
FROM yesterday y
FULL OUTER JOIN today t
ON CAST(y.user_id AS NUMERIC) = t.user_id AND y.browser_type = t.browser_type;