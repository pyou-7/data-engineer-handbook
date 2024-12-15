-- Active: 1732578729425@@127.0.0.1@5432@postgres@public
-- Drop table if exists
DROP TABLE IF EXISTS actors_history_scd;

-- Create SCD Type 2 Table
CREATE TABLE actors_history_scd (
    actor TEXT,
    actorid TEXT,
    streak_identifier INTEGER,
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER,
    current_year INTEGER,
    PRIMARY KEY (actorid, start_date)
);