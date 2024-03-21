# Desenvolvimento do ETL utilizando Python

* Connect - Neste arquivo, estão disponíveis as funções utilizadas para criar a instância e realizar a conexão com o banco de dados local (PostgreSQL) e o banco de dados de onde os dados estão sendo extraídos (MySQL).

Acesse [aqui](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/src/connect_db.py) este script.

![Connect](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/images/connection.png)

* Extract - Este arquivo contém funções que realizam a extração de todas as fontes de dados que retornarão DataFrames utilizando a biblioteca do pandas.

Acesse [aqui](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/src/extract.py) este script.

![Extract](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/images/extract.png)

* Transform - Nesta parte, os DataFrames retornados no script de Extract são comparados com os registros presentes no banco local (DW) para verificar quais deles são registros novos, retornando apenas esses registros.

Acesse [aqui](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/src/transform.py) este script.

![Transform](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/images/transform.png)

* Load - A partir da identificação de novos registros, é determinado se o tipo de carregamento será **Full** ou **Incremental**.

Todos os registros de log contendo as informações de quantidade de registros inseridos e tempo de processamento são adicionados à pasta de log.

Acesse [aqui](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/src/load.py) este script.

![Load](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/images/log.png)


