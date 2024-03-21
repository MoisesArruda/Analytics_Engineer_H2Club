# Desenvolvimento da solução para responder as perguntas de negócio propostas no desafio.

1. Quanto de rake foi gerado por cada Geração* de jogadores?

* Acesse [aqui](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/sql/queries/questao_1.sql) esta querie.

Esta consulta SQL classifica os jogadores em diferentes gerações com base em sua data de nascimento e calcula o total do rake para cada geração. A consulta seleciona dados das tabelas de clientes e resultados, junta esses dados com base no ID do cliente e na tabela de resultados, agrupa os resultados pelo campo de geração e os ordena pelo total do rake em ordem decrescente. Isso fornece uma visão do comportamento de gasto dos jogadores em relação à idade.

![Pergunta 1](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/images/pergunta_1.png)

2. Qual foi o rake gerado por mês? 

* Acesse [aqui](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/sql/queries/questao_2.sql) esta querie.

Esta consulta extrai o ano e o mês da data de acesso dos resultados, calcula o total do rake para cada mês e ano e agrupa os resultados por ano e mês. Em seguida, os resultados são ordenados pelo ano e mês para fornecer uma visão cronológica do total do rake ao longo do tempo.

![Pergunta 2](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/images/pergunta_2.png)

3. Qual sexo tem uma maior proporção de ganhadores**?

* Acesse [aqui](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/sql/queries/questao_3.sql) esta querie.

Esta consulta seleciona o sexo do cliente da tabela cliente e conta o número de identificadores de resultados (id) da tabela resultado onde o valor de ganho (winning) é maior que zero. Os resultados são agrupados pelo sexo do cliente e ordenados pelo número total de ganhadores em ordem decrescente.

![Pergunta 3](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/images/pergunta_3.png)

