SELECT DISTINCT
    candidates.id_number,
    UPPER(CONCAT(candidates.first_name,
                    ' ',
                    candidates.middle_name,
                    ' ',
                    candidates.last_name,
                    ' ',
                    candidates.second_last_name)) AS Nombre,
    CONCAT(schedule.date_start,
            ' ',
            schedule.hour_start) AS fecha_inicio,
    CONCAT(schedule.date_end,
            ' ',
            schedule.hour_end) AS fecha_fin
FROM
    miosv2_ciu_etb.schedule schedule
        LEFT JOIN
    miosv2_rrhh_etb.candidates candidates ON schedule.user_id = candidates.id
WHERE
    (DATE_SUB(NOW(), INTERVAL 5 HOUR) BETWEEN CONCAT(schedule.date_start,
            ' ',
            schedule.hour_start) AND CONCAT(schedule.date_end,
            ' ',
            schedule.hour_end))
        AND hour_start NOT IN ('00:00:00')
        AND schedule.schedule_type_id = 1;