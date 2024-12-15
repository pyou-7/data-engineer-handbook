-- Active: 1732578729425@@127.0.0.1@5432@postgres@public
CREATE TYPE actor_scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER
);

INSERT INTO actors_history_scd
WITH last_year_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE current_year = (SELECT MAX(current_year) FROM actors_history_scd)
),
current_year_data AS (
    SELECT *
    FROM actors
    WHERE current_year = (SELECT MAX(current_year) + 1 FROM actors_history_scd)
),
unchanged_records AS (
    SELECT 
        cy.actor,
        cy.actorid,
        cy.quality_class,
        cy.is_active,
        ly.start_date,
        cy.current_year AS end_date
    FROM current_year_data cy
    JOIN last_year_scd ly
    ON ly.actorid = cy.actorid
    WHERE cy.quality_class = ly.quality_class
    AND cy.is_active = ly.is_active
),
changed_records AS (
    SELECT 
        cy.actor,
        cy.actorid,
        cy.quality_class,
        cy.is_active,
        cy.current_year AS start_date,
        cy.current_year AS end_date,
        UNNEST(ARRAY[
            ROW(
                ly.quality_class,
                ly.is_active,
                ly.start_date,
                ly.end_date
            )::actor_scd_type,
            ROW(
                cy.quality_class,
                cy.is_active,
                cy.current_year,
                cy.current_year
            )::actor_scd_type
        ]) AS records
    FROM current_year_data cy
    LEFT JOIN last_year_scd ly
    ON ly.actorid = cy.actorid
    WHERE cy.quality_class <> ly.quality_class
    OR cy.is_active <> ly.is_active
),
unnested_changed_records AS (
    SELECT 
        actor,
        actorid,
        (records::actor_scd_type).quality_class,
        (records::actor_scd_type).is_active,
        (records::actor_scd_type).start_date,
        (records::actor_scd_type).end_date
    FROM changed_records
),
new_records AS (
    SELECT 
        cy.actor,
        cy.actorid,
        cy.quality_class,
        cy.is_active,
        cy.current_year AS start_date,
        cy.current_year AS end_date
    FROM current_year_data cy
    LEFT JOIN last_year_scd ly
    ON cy.actorid = ly.actorid
    WHERE ly.actorid IS NULL
)
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM unnested_changed_records
UNION ALL
SELECT * FROM new_records;