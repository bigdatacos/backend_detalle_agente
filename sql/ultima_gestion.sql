SELECT 
    t1.user_id, 
    t1.end AS start
FROM
    `miosv2-phone`.calls t1
JOIN (
    SELECT 
        user_id,
        MAX(start) AS max_start
    FROM
        `miosv2-phone`.calls
    WHERE
        end IS NOT NULL
        AND DATE(start) = CURDATE()
    GROUP BY 
        user_id
) mst
ON 
    t1.user_id = mst.user_id
    AND t1.start = mst.max_start;