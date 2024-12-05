SELECT 
    id, rrhh_id, extension, document as Cedula_usersv2, username as Usuario, updated_at as updated_at_users
FROM
    `miosv2-phone`.usersv2 ORDER BY id desc;