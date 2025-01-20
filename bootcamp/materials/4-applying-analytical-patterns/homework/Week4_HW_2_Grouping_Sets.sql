DROP TABLE IF EXISTS game_details_dashboard;

-- Use GROUPING SETS to create a table that shows the total points scored by each player for each team, each season, and overall.
CREATE TABLE game_details_dashboard AS
WITH game_details_augmented AS (
    SELECT
        gd.game_id,
        gd.team_id,
        gd.player_name,
        gd.pts,
        g.season,
        gd.team_abbreviation,
        gd.team_city,
        gd.plus_minus      
    FROM game_details gd
    LEFT JOIN games g 
    ON gd.game_id = g.game_id
    WHERE pts IS NOT NULL
)
SELECT
    CASE
        WHEN GROUPING(player_name) = 0 AND GROUPING(team_abbreviation) = 0 THEN 'player_team'
        WHEN GROUPING(player_name) = 0 AND GROUPING(season) = 0 THEN 'player_season'
        WHEN GROUPING(game_id) = 0 AND GROUPING(team_abbreviation) = 0 THEN 'game_team'
    END AS aggregation_level,
    COALESCE(player_name, '(overall)') AS player_name,
    COALESCE(team_abbreviation, '(overall)') AS team_abbreviation,
    COALESCE(CAST(game_id AS TEXT), '(overall)') AS game_id,
    COALESCE(CAST(season AS TEXT), '(overall)') AS season,
    SUM(pts) AS total_points,
    SUM(plus_minus) AS total_plus_minus
FROM game_details_augmented
GROUP BY GROUPING SETS (
    (player_name, team_abbreviation),
    (player_name, season),
    (game_id, team_abbreviation)
)
ORDER BY total_points DESC;

-- Who scored the most points playing for one team?
SELECT * 
FROM game_details_dashboard 
WHERE aggregation_level = 'player_team' 
ORDER BY total_points DESC;

-- Who scored the most points in one season?
SELECT * 
FROM game_details_dashboard 
WHERE aggregation_level = 'player_season' 
ORDER BY total_points DESC;

-- Which team has won the most games?
SELECT
    team_abbreviation AS team_name,
    COUNT(DISTINCT game_id) AS total_wins
FROM game_details_dashboard 
WHERE aggregation_level = 'game_team' AND total_plus_minus > 0
GROUP BY team_abbreviation
ORDER BY total_wins DESC;