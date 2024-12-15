-- Active: 1732578729425@@127.0.0.1@5432@postgres@public
-- Check the data
SELECT * FROM actor_films WHERE actor = 'Jackie Chan' and year = 1970;

-- Create Custom Types
-- Quality Class Type to categorize actor performance
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- Film Struct Type to represent film details
CREATE TYPE film_struct AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

-- Drop table if it exists
DROP TABLE IF EXISTS actors;
-- DDL for actors table
CREATE TABLE actors (
    actor TEXT,
    actorid TEXT,
    films film_struct[],
    quality_class quality_class,
    is_active BOOLEAN,
    current_year INTEGER,
    PRIMARY KEY (actorid, current_year)
);

-- Populate actors table
DO $$ 
DECLARE
    current_year_var INTEGER := (SELECT MIN(year) FROM actor_films); -- Start year
BEGIN
    -- Loop through all years from the minimum year to the maximum year in actor_films
    WHILE current_year_var <= (SELECT MAX(year) FROM actor_films) LOOP
        -- Your existing query with renamed variable
        WITH yesterday AS (
            SELECT * 
            FROM actors
            WHERE current_year = current_year_var - 1
        ),
        today AS (
            SELECT
                actor,
                actorid,
                year,
                ARRAY_AGG(ROW(film, votes, rating, filmid)::film_struct) AS films,
                CASE 
                    WHEN AVG(rating) > 8 THEN 'star'
                    WHEN AVG(rating) > 7 THEN 'good'
                    WHEN AVG(rating) > 6 THEN 'average'
                    ELSE 'bad'
                END::quality_class,
                TRUE AS is_active  -- Indicating that the actor is active in the current year
            FROM actor_films
            WHERE year = current_year_var
            GROUP BY actor, actorid, year
        )
        INSERT INTO actors
        SELECT
            COALESCE(t.actor, y.actor) AS actor,
            COALESCE(t.actorid, y.actorid) AS actorid,
            CASE 
                WHEN y.films IS NULL THEN t.films
                WHEN t.films IS NOT NULL THEN y.films || t.films
                ELSE y.films
            END AS films,
            COALESCE(t.quality_class, y.quality_class) AS quality_class,
            COALESCE(t.is_active, FALSE) AS is_active,
            current_year_var AS current_year
        FROM today t
        FULL OUTER JOIN yesterday y 
            ON t.actor = y.actor AND t.actorid = y.actorid;
        -- Increment the year variable
        current_year_var := current_year_var + 1;
    END LOOP;
END $$;

-- Check if the data are inserted correctly
SELECT *
FROM actors
WHERE actor = 'James Fox';




