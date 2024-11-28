SELECT 
    t1.user_id, t1.start
FROM
    `miosv2-phone`.calls t1
WHERE
    t1.start = (SELECT 
            MAX(t2.start)
        FROM
            `miosv2-phone`.calls t2
        WHERE
            t2.user_id = t1.user_id
                AND DATE(t2.start) = CURDATE());