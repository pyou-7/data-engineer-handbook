INSERT INTO host_activity_reduced
WITH daily_aggregate AS (
    SELECT
        host,
        DATE_TRUNC('day', CAST(event_time AS DATE)) AS date,
        COUNT(1) AS daily_hits,
        COUNT(DISTINCT user_id) AS daily_unique_visitors
    FROM events
    GROUP BY host, DATE_TRUNC('day', CAST(event_time AS DATE))
),
yesterday_array AS (
    SELECT *
    FROM host_activity_reduced
    WHERE month = DATE('2023-01-01')
),
combined_data AS (
    SELECT
        COALESCE(da.host, ya.host) AS host,
        COALESCE(ya.month, DATE_TRUNC('month', da.date)) AS month,
        CASE
            WHEN ya.hit_array IS NOT NULL THEN
                ya.hit_array || ARRAY[COALESCE(da.daily_hits, 0)]
            ELSE
                ARRAY_FILL(0, ARRAY[COALESCE(EXTRACT('day' FROM DATE(da.date) - DATE_TRUNC('month', da.date)), 0)::INTEGER]) || ARRAY[COALESCE(da.daily_hits, 0)]
        END AS hit_array,
        CASE
            WHEN ya.unique_visitors IS NOT NULL THEN
                ya.unique_visitors || ARRAY[COALESCE(da.daily_unique_visitors, 0)]
            ELSE
                ARRAY_FILL(0, ARRAY[COALESCE(EXTRACT('day' FROM DATE(da.date) - DATE_TRUNC('month', da.date)), 0)::INTEGER]) || ARRAY[COALESCE(da.daily_unique_visitors, 0)]
        END AS unique_visitors
    FROM daily_aggregate da
    FULL OUTER JOIN yesterday_array ya
    ON da.host = ya.host
),
deduplicated_data AS (
    SELECT
        host,
        month,
        MAX(hit_array) AS hit_array,
        MAX(unique_visitors) AS unique_visitors
    FROM combined_data
    GROUP BY host, month
)
SELECT host, month, hit_array, unique_visitors
FROM deduplicated_data
ON CONFLICT (month, host)
DO UPDATE SET
    hit_array = EXCLUDED.hit_array,
    unique_visitors = EXCLUDED.unique_visitors;
