SELECT 
    candidates.id,
    concat(ciu.date_start, " ", ciu.hour_start) as fecha_inicio_pausa,
		
    ciu.date_end,
    ciu.hour_end,
    CONCAT(candidates.first_name,
            ' ',
            candidates.middle_name,
            ' ',
            candidates.last_name,
            ' ',
            candidates.second_last_name) AS Nombre,
    candidates.id_number AS Cedula,
    schedule_type.name AS Estado
FROM
    `miosv2_ciu_etb`.ciu ciu
        LEFT JOIN
    `miosv2_ciu_etb`.schedule_types schedule_type ON ciu.schedule_type_id = schedule_type.id
        LEFT JOIN
    `miosv2_ciu_etb`.schedule schedule ON ciu.schedule_id = schedule.id
        LEFT JOIN
    miosv2_rrhh_etb.candidates candidates ON schedule.user_id = candidates.id
WHERE
    schedule_type.name NOT IN ('jornada laboral')
        AND (ciu.date_end IS NULL
        OR ciu.hour_end IS NULL)
        AND ciu.date_start = CURDATE()
ORDER BY ciu.created_at DESC;