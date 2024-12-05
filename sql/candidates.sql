SELECT 
    id as id_candidates,
    UPPER(CONCAT(candidates.first_name,
            ' ',
            candidates.middle_name,
            ' ',
            candidates.last_name,
            ' ',
            candidates.second_last_name)) AS Nombre,
    candidates.id_number AS Cedula
FROM
    miosv2_rrhh_etb.candidates candidates;