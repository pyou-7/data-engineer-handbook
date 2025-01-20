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
        DISTINCT player_name,
        game_id,
        pts,
        CASE 
            WHEN pts > 10 THEN 1 
            ELSE 0 
        END AS scored_above_10
    FROM game_details
    WHERE player_name = 'LeBron James'
),
streaks AS (
    SELECT
        player_name,
        game_id,
        pts,
        scored_above_10,
        -- Identify streak resets by incrementing a counter when LeBron scores <= 10
        SUM(CASE 
                WHEN scored_above_10 = 0 THEN 1 
                ELSE 0 
            END) OVER (
            PARTITION BY player_name
            ORDER BY game_id
        ) AS streak_reset
    FROM lebron_games
),
scoring_streaks AS (
    SELECT
        player_name,
        streak_reset,
        COUNT(*) AS streak_length
    FROM streaks
    WHERE scored_above_10 = 1 -- Only consider games where LeBron scored > 10 points
    GROUP BY player_name, streak_reset
)
SELECT 
    player_name, 
    MAX(streak_length) AS longest_streak
FROM scoring_streaks
GROUP BY player_name;




SELECT * FROM game_details WHERE team_abbreviation = 'LAL' ORDER BY game_id;

with dedupes as (
select
    distinct game_id,
    pts,
    player_name
from 
game_details gd 
where player_name = 'LeBron James'
)
, streak as (select 
    game_id,
    case when pts > 10 then 1 
    when pts <= 10 then 0
    else null end as pts_over_10,
    pts,
    sum(case when pts > 10 then 0 
    when pts <= 10 then 1
    else 1 end) over(partition by player_name ORDER BY game_id
                    ROWS BETWEEN UNBOUNDED PRECEDING AND current row) as streak
from 
dedupes gd )
select
    *
from streak
order by game_id;


WITH lebron_games AS (
    SELECT
        gd.player_name,
        gd.game_id,
        g.game_date_est,
        gd.pts,
        CASE 
            WHEN gd.pts > 10 THEN 1 
            ELSE 0 
        END AS scored_above_10
    FROM game_details gd
    JOIN games g
    ON gd.game_id = g.game_id
    WHERE gd.player_name = 'LeBron James'
),
game_numbers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY game_date_est) AS row_number
    FROM lebron_games
),
streaks AS (
    SELECT
        game_id,
        player_name,
        game_date_est,
        pts,
        scored_above_10,
        row_number,
        row_number - SUM(scored_above_10) OVER (
            PARTITION BY player_name
            ORDER BY row_number
        ) AS streak_group
    FROM game_numbers
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
