SELECT 
    mst.user_id, MAX(mst.max_end) AS start
FROM
    (SELECT 
        transfers.user_id, MAX(calls.end) AS max_end
    FROM
        `miosv2-phone`.transfers transfers
    LEFT JOIN `miosv2-phone`.calls calls ON transfers.call_id = calls.id
    WHERE
        calls.end IS NOT NULL
            AND DATE(calls.end) = CURDATE()
            AND transfers.user_id IS NOT NULL
    GROUP BY transfers.user_id UNION SELECT 
        user_id, MAX(end) AS max_end
    FROM
        `miosv2-phone`.calls
    WHERE
        end IS NOT NULL
            AND DATE(end) = CURDATE()
            AND user_id IS NOT NULL
    GROUP BY user_id) mst
GROUP BY mst.user_id;