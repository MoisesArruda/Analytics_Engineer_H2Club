SELECT
    EXTRACT(YEAR FROM data_acesso) AS ano,
    EXTRACT(MONTH FROM data_acesso) AS mes,
    SUM(rake) AS rake_por_mes
FROM
    ft.resultado
GROUP BY
    ano, mes
ORDER BY
    ano, mes;