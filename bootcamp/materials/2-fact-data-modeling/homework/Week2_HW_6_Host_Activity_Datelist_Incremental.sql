DO $$
DECLARE
    current_loop_date DATE := '2023-01-01'; -- Starting date
    end_date DATE := '2023-01-31';          -- Ending date
BEGIN
    -- Loop through each date from 2023-01-01 to 2023-01-31
    WHILE current_loop_date <= end_date LOOP
        WITH last_day_cumulated AS (
            -- Fetch the last recorded data for the previous day
            SELECT *
            FROM hosts_cumulated
            WHERE date = current_loop_date - INTERVAL '1 day'
        ),
        current_day_data AS (
            -- Fetch today's activity from the events table
            SELECT 
                host AS host_id,
                DATE_TRUNC('day', CAST(event_time AS TIMESTAMP)) AS today_date
            FROM events
            WHERE DATE_TRUNC('day', CAST(event_time AS TIMESTAMP)) = current_loop_date
            GROUP BY host, DATE_TRUNC('day', CAST(event_time AS TIMESTAMP))
        ),
        updated_hosts AS (
            -- Update the activity list for existing hosts
            SELECT 
                ld.host_id,
                CASE 
                    WHEN cd.today_date IS NOT NULL THEN ld.host_activity_datelist || ARRAY[cd.today_date]
                    ELSE ld.host_activity_datelist
                END AS host_activity_datelist,
                current_loop_date AS date
            FROM last_day_cumulated ld
            LEFT JOIN current_day_data cd
            ON ld.host_id = cd.host_id
        ),
        new_hosts AS (
            -- Insert new hosts that are not in the previous day's data
            SELECT 
                cd.host_id,
                ARRAY[cd.today_date] AS host_activity_datelist,
                cd.today_date AS date
            FROM current_day_data cd
            LEFT JOIN last_day_cumulated ld
            ON cd.host_id = ld.host_id
            WHERE ld.host_id IS NULL
        )
        -- Insert updated and new records into hosts_cumulated
        INSERT INTO hosts_cumulated (host_id, host_activity_datelist, date)
        SELECT * FROM updated_hosts
        UNION ALL
        SELECT * FROM new_hosts;

        -- Increment the current_loop_date
        current_loop_date := current_loop_date + INTERVAL '1 day';
    END LOOP;
END $$;
