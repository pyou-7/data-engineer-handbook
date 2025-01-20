DROP TABLE IF EXISTS players_state_tracking;

-- Create the table to track the state of players
CREATE TABLE players_state_tracking (
    player_name text,
    first_active_season integer,
    last_active_season integer,
    season_state TEXT,
    current_season integer,
    primary key (player_name, current_season)
 );

-- Populate the table with the state of players
INSERT INTO players_state_tracking
WITH yesterday AS (
    SELECT *
    FROM players_state_tracking
    WHERE current_season = 2009
),
active_windows AS (
    SELECT
        player_name,
        current_season,
        is_active,
        LAG(is_active) over (partition by player_name ORDER BY current_season) as prev_is_active,
        LEAD(is_active) over (partition by player_name ORDER BY current_season) as next_is_active
    FROM players
),
today AS (
    SELECT
        player_name,
        current_season,
        is_active,
        prev_is_active,
        next_is_active
    FROM active_windows
    WHERE current_season = 2010
)
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(y.first_active_season, t.current_season) AS first_active_season,
    CASE
        WHEN t.is_active = TRUE THEN COALESCE(t.current_season, y.last_active_season) 
        ELSE y.last_active_season 
        END AS last_active_season,
    CASE
        WHEN t.is_active = TRUE AND t.prev_is_active IS NULL THEN 'New'
        WHEN t.is_active = FALSE AND t.prev_is_active = TRUE THEN 'Retired'
        WHEN t.is_active = TRUE AND t.prev_is_active = TRUE THEN 'Continued Playing'
        WHEN t.is_active = TRUE AND t.prev_is_active = FALSE THEN 'Returned from Retirement'
        WHEN t.is_active = FALSE AND t.prev_is_active = FALSE THEN 'Stayed Retired'
        ELSE 'Stale' END AS season_state,
    COALESCE(t.current_season, y.current_season + 1) AS current_season
FROM today t
FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name;

-- Check the state of a player
SELECT * FROM players_state_tracking
WHERE player_name = 'Kevin Durant';