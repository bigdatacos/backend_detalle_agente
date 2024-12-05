SELECT 
    operation_user.user_id as user_id_operation, operations.name AS Campana
FROM
    `miosv2-phone`.operation_user operation_user
        LEFT JOIN
    `miosv2-phone`.operations operations ON operation_user.operation_id = operations.id;