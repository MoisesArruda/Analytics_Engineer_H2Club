SELECT
    CASE
        WHEN EXTRACT(YEAR FROM c.data_nascimento) BETWEEN 1925 AND 1940 THEN 'Veteranos'
        WHEN EXTRACT(YEAR FROM c.data_nascimento) BETWEEN 1941 AND 1959 THEN 'Baby Boomers'
        WHEN EXTRACT(YEAR FROM c.data_nascimento) BETWEEN 1960 AND 1979 THEN 'Geração X'
        WHEN EXTRACT(YEAR FROM c.data_nascimento) BETWEEN 1980 AND 1995 THEN 'Geração Y'
        WHEN EXTRACT(YEAR FROM c.data_nascimento) BETWEEN 1996 AND 2010 THEN 'Geração Z'
        ELSE 'Geração Alpha'
    END AS geracao,
    SUM(CASE WHEN t.winning > 0 THEN t.rake ELSE 0 END) AS rake_por_geracao
FROM
    dim.cliente c
JOIN
    ft.resultado t ON c.id = t.clientes_id
GROUP BY
    geracao
ORDER BY
    rake_por_geracao DESC;