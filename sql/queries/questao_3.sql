SELECT
    c.sexo,
    COUNT(t.id) AS total_ganhadores
FROM
    dim.cliente c
JOIN
    ft.resultado t ON c.id = t.clientes_id
WHERE
    t.winning > 0
GROUP BY
    c.sexo
ORDER BY
    total_ganhadores DESC;