SELECT 
    calls.user_id,
    calls.start,
    calls.phone_number as Telefono,
    operations.name AS Campana,
    campaigns.name AS Skill
FROM
    `miosv2-phone`.calls calls
        LEFT JOIN
    `miosv2-phone`.campaigns campaigns ON calls.campaign_id = campaigns.id
        LEFT JOIN
    `miosv2-phone`.operations operations ON campaigns.operation_id = operations.id
WHERE
    DATE(start) = CURDATE() AND end IS NULL
        AND calls.user_id IS NOT NULL
        AND calls.state_id = 1
        AND calls.type_id = 1
        AND calls.id NOT IN (SELECT 
            call_id
        FROM
            `miosv2-phone`.transfers
        WHERE
            DATE(created_at) = CURDATE()
        ORDER BY id DESC);