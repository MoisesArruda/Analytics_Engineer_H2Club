# Implementando um Projeto de ETL proposto pela H2 Club.

Este repositório contém o código e a documentação para a construção de um projeto de ETL (Extração, Transformação e Carga).

* [Acesse aqui](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/tree/main/sql) a página de explicação das queries realizadas para responder as perguntas

* [Acesse aqui](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/tree/main/src) a página de explicação dos scripts python.

## Tecnologias Utilizadas:

**DBeaver** : Ferramenta que permite acessar diferentes tipos de banco de dados.

**MySQL** : Banco de dados onde foi realizada a extração dos dados.

**PostgreSQL** : Banco de dados utilizado para armazenar os dados.

**Python 3.10.10** : Linguagem de programação usada para a automação do ETL.

**Podman**: Utilizado para desenvolver, gerenciar e executar contêineres.

**SQL**: Linguagem de consulta usada para transformar e carregar os dados do Banco de Dados.

### Instalação das Tecnologias:

* [DBeaver Community](https://dbeaver.io/download/)

* [PostgresSQL](https://www.postgresql.org/)

* [Podman download](https://podman.io/)

* [Python 3.10.10](https://www.python.org/downloads/release/python-31010/)

Se estiver utilizando o *Pyenv* como gerenciador de versões Python:

``` bash
pyenv local 3.10.10
```

## Rode o Projeto

1. Clone o repositório:

```bash
git clone https://github.com/MoisesArruda/Analytics_Engineer_H2Club
cd Analytics_Engineer
```


2. Crie o ambiente virtual e ative-o:
```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
```

3. Instale as dependências do projeto:
```bash
pip install -r requirements.txt
```

4. Crie um arquivo .env e passe as configurações do Banco de Dados enviadas por e-mail.
```bash
MYSQL_HOST = "YOUR_MYSQL_HOST"
MYSQL_USER = "YOUR_MYSQL_USER"
MYSQL_PASSWORD = "YOUR_MYSQL_PASSWORD"
MYSQL_PORT = "YOUR_MYSQL_PORT"
MYSQL_DATABASE = "YOUR_MYSQL_DATABASE"
MYSQL_TABLE = "YOUR_MYSQL_TABLE"
```

5. No terminal de comando de sua preferência, inicie o podman:

``` bash 
podman machine init
```

``` bash
podman machine start
```

6. Puxe a imagem do MySql do docker hub.

``` bash
podman pull mysql:5.7 
```

7. Rode a imagem na porta 3307:3306.
 ``` bash
podman run --name mysql -e MYSQL_ROOT_PASSWORD=password -p 3307:3306 -d mysql:5.7
 ```

8. Conecte-se ao MySql pelo DBeaver:

![db connect](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/images/MySql_connect.pnggit )

9. Crie os Schemas e as tabelas necessárias para este projeto:

* Utilize [este script](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/sql/create_schema.sql) para criar os **Schemas** no Banco de Dados.

* Utilize [este script](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/sql/dim_cliente.sql) para criar a tabela **cliente** no Banco de Dados.

* Utilize [este script](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/sql/ft_resultado.sql) para criar a tabela **resultado** no Banco de Dados.

* Utilize [este script](https://github.com/MoisesArruda/Analytics_Engineer_H2Club/blob/main/sql/consolidated_data.sql) para criar a tabela **consolidated data** no Banco de Dados.


* Para pausar o MySql
```bash
podman pause mysql:5.7
```

## Contato
Para dúvidas, sugestões ou feedbacks:

* **Moisés Arruda** - moises_arruda@outlook.com