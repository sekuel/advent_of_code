WITH input AS (
    SELECT val 
    FROM read_csv_auto('~/01/input.csv', columns = { val: text })  
), replaced AS (
    SELECT 
        (val)
        .replace('twone', 'twoone')
        .replace('eighthree', 'eightthree')
        .replace('oneight', 'oneeight')
        .replace('threeight', 'threeeight')
        .replace('fiveight', 'fiveeight')
        .replace('nineight', 'nineeight')
        .replace('eightwo', 'eighttwo')
        .replace('sevenine', 'sevennine') AS replaced_val
    FROM input
), extracted AS (
    SELECT 
        replaced_val, 
        list_transform(
            regexp_extract_all(replaced_val, 'one|eight|three|four|five|six|seven|two|nine|\d'), 
                x ->
                    CASE x 
                        WHEN 'one' THEN 1 
                        WHEN 'two' THEN 2
                        WHEN 'three' THEN 3
                        WHEN 'four' THEN 4
                        WHEN 'five' THEN 5
                        WHEN 'six' THEN 6
                        WHEN 'seven' THEN 7
                        WHEN 'eight' THEN 8
                        WHEN 'nine' THEN 9
                        ELSE x 
                    END
            )  AS extracted_value 
    FROM replaced
)
SELECT sum((extracted_value[1] || extracted_value[-1])::int) AS total FROM extracted;