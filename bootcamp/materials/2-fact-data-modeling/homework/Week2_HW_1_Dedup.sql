-- Active: 1732578729425@@127.0.0.1@5432@postgres@public
-- Create a temporary table to store the deduplicated game details
CREATE TEMPORARY TABLE temp_game_details AS
WITH deduplicated_game_details AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id) AS row_num
    FROM game_details gd
)
SELECT *
FROM deduplicated_game_details
WHERE row_num = 1;

-- Insert the deduplicated data into the game_details table
TRUNCATE TABLE game_details;

INSERT INTO game_details
SELECT game_id, team_id, team_abbreviation, team_city, player_id, player_name, nickname,
start_position, comment, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, 
reb, ast, stl, blk, "TO", pf, pts, plus_minus FROM temp_game_details;
