SELECT 
        id, rrhh_id, extension, document as Cedula, username as Usuario
    FROM
        `miosv2-phone`.usersv2
    WHERE
        extension IS NOT NULL