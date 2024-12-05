SELECT 
    calls.user_id,
    calls.start,
    operations.name as Campana,
    campaigns.name as Skill
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