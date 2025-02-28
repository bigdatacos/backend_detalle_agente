SELECT 
    id, rrhh_id, extension, document AS Cedula_usersv2, username AS Usuario, updated_at AS updated_at_users
FROM
    `miosv2-phone`.usersv2
WHERE
    (extension IS NOT NULL AND TRIM(extension) != '') AND available = 1 
ORDER BY
    id DESC;