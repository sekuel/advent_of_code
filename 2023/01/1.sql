WITH input AS (
    SELECT * 
    FROM read_csv_auto('~/01/input.csv', columns = { val: text })  
), extracted AS (
    SELECT regexp_extract_all(val, '\d') AS extracted_value FROM input
)
SELECT sum((extracted_value[1] || extracted_value[-1])::int) AS total FROM extracted;