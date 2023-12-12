WITH RECURSIVE input AS (
    SELECT
        row_number() OVER () AS rn,
        column0,
    FROM read_csv_auto('~/08/input.csv', sep='')
), directions AS (
    SELECT
        unnest(split(column0, '')) AS direction,
        generate_subscripts(split(column0, ''), 1) - 1 AS step,
        len(column0.regexp_extract_all('\w')) AS max_steps
    FROM input
    WHERE rn = 1
), networks AS (
    SELECT 
        regexp_extract_all(column0, '\w{3}')[1] AS node,
        regexp_extract_all(column0, '\w{3}')[2] AS l,
        regexp_extract_all(column0, '\w{3}')[3] AS r
    FROM input
    WHERE rn > 2
), walk AS (
    SELECT
        0 AS n,
        networks.node AS node,
        if(directions.direction = 'L', networks.l, networks.r) AS _next
    FROM networks, directions
    WHERE networks.node = 'AAA' 
        AND directions.step = 0
    UNION ALL
    SELECT
        walk.n + 1 AS n,
        walk._next AS node,
        if(directions.direction = 'L', networks.l, networks.r) AS _next
    FROM walk
    JOIN networks ON walk._next = networks.node
    JOIN directions ON directions.step = ((walk.n + 1) % directions.max_steps)
    WHERE walk._next != 'ZZZ'
), walk_2 AS (
    SELECT
        0 AS n,
        networks.node AS first_node,
        networks.node AS node,
        if(directions.direction = 'L', networks.l, networks.r) AS _next
    FROM networks, directions
    WHERE networks.node[3] = 'A' 
        AND networks.node[3] != 'Z'
        AND directions.step = 0 
    UNION ALL
    SELECT
        walk_2.n + 1 AS n,
        walk_2.first_node,
        walk_2._next AS node,
        if(directions.direction = 'L', networks.l, networks.r) AS _next
    FROM walk_2
    JOIN networks 
        ON walk_2._next = networks.node
        AND networks.node[3] != 'Z'
    JOIN directions ON directions.step = ((walk_2.n + 1) % directions.max_steps)
), max_walks AS materialized (
    SELECT 
        max(n)+1 AS max_step 
    FROM walk_2 
    GROUP BY first_node
), max_walks_id AS materialized (
    SELECT *, row_number() OVER () - 1 AS id FROM max_walks
), second_walk AS (
    SELECT id, max_step::int64 AS lcm FROM max_walks_id
    UNION ALL
    SELECT
        max_walks_id.id,
        lcm(max_walks_id.max_step, second_walk.lcm)::int64 AS lcm
    FROM second_walk
    JOIN max_walks_id
        ON second_walk.id + 1 = max_walks_id.id
)
SELECT 'Part I' AS parts, count(n) AS answer FROM walk
UNION ALL
SELECT 'Part II' AS parts, max(lcm) AS answer FROM second_walk;