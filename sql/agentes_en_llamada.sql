SELECT 
    user_id, start
FROM
    `miosv2-phone`.calls
WHERE
    DATE(start) = CURDATE() AND end IS NULL
        AND user_id IS NOT NULL