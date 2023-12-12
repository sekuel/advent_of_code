WITH input AS (
    SELECT
        split(split(cubes, ':')[1], ' ')[2]::INT AS game_id,
        split(replace(split(cubes, ': ')[2], ';', ','), ', ') AS cubes
    FROM READ_CSV('~/02/input.csv', columns = { cubes: text }, delim = '')
), unnested AS (
    SELECT
        game_id,
        unnest(cubes) AS cubes
    FROM input
), cubes AS (
    SELECT
        game_id,
        split(cubes, ' ')[1]::INT AS cubes_amount,
        split(cubes, ' ')[2] AS cubes_color
    FROM unnested
), possible_cubes AS (
    SELECT
        game_id,
        cubes_color,
        max(cubes_amount) AS min_cubes
    FROM cubes
    GROUP BY 1, 2
), powers AS (
    SELECT
        game_id,
        product(min_cubes) AS _power
    FROM possible_cubes
    GROUP BY 1
)
SELECT sum(_power) AS total_power FROM powers;
