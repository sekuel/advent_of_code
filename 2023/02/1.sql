WITH input AS (
    SELECT
        split(split(cubes, ':')[1], ' ')[2]::int AS game_id,
        split(replace(split(cubes, ': ')[2], ';', ','), ', ') AS cubes
    FROM read_csv('~/02/input.csv', columns = { cubes: text }, DELIM = '')
), unnested AS (
    SELECT
        game_id,
        unnest(cubes) AS cubes
    FROM input
), cubes AS (
    SELECT
        game_id,
        split(cubes, ' ')[1] AS cubes_amount,
        split(cubes, ' ')[2] AS cubes_color
    FROM unnested
), impossible_games AS (
            SELECT game_id FROM cubes WHERE (cubes_color = 'red' and cubes_amount > 12)
    UNION   SELECT game_id FROM cubes WHERE (cubes_color = 'green' and cubes_amount > 13)
    UNION   SELECT game_id FROM cubes WHERE (cubes_color = 'blue' and cubes_amount > 14)
)
SELECT sum(DISTINCT game_id) AS games
FROM cubes 
WHERE game_id NOT IN (SELECT game_id FROM impossible_games);