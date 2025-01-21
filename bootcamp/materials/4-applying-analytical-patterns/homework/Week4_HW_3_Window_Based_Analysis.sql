-- What is the most games a team has won in a 90 game stretch?
WITH game_won_aggregated AS(
    SELECT
        team_abbreviation,
        game_id,
        CASE
            WHEN SUM(plus_minus) > 0 THEN 1
            ELSE 0
        END AS win
    FROM game_details
    GROUP BY team_abbreviation, game_id
),
games_won_windowed AS (
    SELECT
        ga.team_abbreviation,
        ga.game_id,
        SUM(win) OVER (
            PARTITION BY ga.team_abbreviation
            ORDER BY g.game_date_est
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_last_90_games
    FROM game_won_aggregated ga
    LEFT JOIN games g
    ON ga.game_id = g.game_id
)
SELECT  
    team_abbreviation, 
    MAX(wins_in_last_90_games) AS max_wins_in_90_games
FROM games_won_windowed
GROUP BY team_abbreviation
ORDER BY max_wins_in_90_games DESC
LIMIT 1;


-- How many games in a row did LeBron James score over 10 points a game?
WITH lebron_games AS (
    SELECT
        gd.player_name,
        g.game_date_est,
        CASE 
            WHEN gd.pts > 10 THEN 1 
            ELSE 0 
        END AS scored_above_10,
        ROW_NUMBER() OVER (PARTITION BY gd.player_name ORDER BY g.game_date_est) AS row_number
    FROM game_details gd
    JOIN games g
    ON gd.game_id = g.game_id
    WHERE gd.player_name = 'LeBron James'
),
streaks AS (
    SELECT
        player_name,
        row_number - SUM(scored_above_10) OVER (
            PARTITION BY player_name
            ORDER BY row_number
        ) AS streak_group
    FROM lebron_games
    WHERE scored_above_10 = 1
),
streak_lengths AS (
    SELECT
        player_name,
        streak_group,
        COUNT(*) AS streak_length
    FROM streaks
    GROUP BY player_name, streak_group
)
SELECT 
    player_name,
    MAX(streak_length) AS longest_streak
FROM streak_lengths
GROUP BY player_name;
