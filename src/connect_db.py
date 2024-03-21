# Pacote de conexão.

import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine,exc
import psycopg2
import pymysql

load_dotenv()


def connect_db_source():

    """
    Obtém as configurações de conexão ao banco de dados source a partir das variáveis de ambiente.

    Returns:
        dict or None: Um dicionário contendo as configurações de conexão (database, user, password, host, port).
                      Retorna None se ocorrer um erro ao obter as configurações.
    """
     
    try:
        db_source_config = {
            "database": os.getenv("MYSQL_DATABASE"),
            "user": os.getenv("MYSQL_USER"),
            "password": os.getenv("MYSQL_PASSWORD"),
            "host": os.getenv("MYSQL_HOST"),
            "port": int(os.getenv("MYSQL_PORT"))
        }

        # Verifica se todas as variáveis de ambiente estão definidas
        if None in db_source_config.values():
            raise ValueError("Certifique-se de que todas as variáveis de ambiente estão definidas.")

        return db_source_config

    except Exception as e:
        # Lidar com exceções (por exemplo, variáveis de ambiente não definidas)
        print(f"Erro ao obter configurações de conexão: {str(e)}")
        return None


def connect_db_dw():

    """
    Retorna as configurações de conexão ao banco de dados data warehouse (DW).

    Returns:
        dict: Um dicionário contendo as configurações de conexão (database, user, password, host, port).
              Retorna um dicionário padrão se ocorrer um erro ao obter as configurações.
    """
    try:
        db_dw_config = {
            "database": "dw",
            "user": "postgres",
            "password": "1234",
            "host": "localhost",
            "port": "5434"
        }

        # Você pode adicionar verificações adicionais ou tratamento de exceções aqui, se necessário.

        return db_dw_config

    except Exception as e:
        # Lidar com exceções (por exemplo, problemas ao obter configurações)
        print(f"Erro ao obter configurações de conexão com o DW: {str(e)}")
        return {
            "database": "dw",
            "user": "postgres",
            "password": "1234",
            "host": "localhost",
            "port": "5434"
        }


def create_database_engine_dw(db_params):

    """
    Cria e retorna uma instância de engine para conexão ao banco de dados PostgreSQL.

    Args:
        db_params (dict): Um dicionário contendo as configurações de conexão (user, password, host, port, database).

    Returns:
        sqlalchemy.engine.base.Engine: Uma instância de engine para conexão ao banco de dados PostgreSQL.
                                     Retorna None em caso de erro ao criar a engine.
    """
    try:
        engine = create_engine('postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
            db_params["user"],
            db_params["password"],
            db_params["host"],
            db_params["port"],
            db_params["database"]
        ))

        return engine

    except exc.SQLAlchemyError as e:
        # Lidar com exceções relacionadas ao SQLAlchemy (por exemplo, problemas ao criar a engine)
        print(f"Erro ao criar engine PostgreSQL: {str(e)}")
        return None
    

def create_database_engine_source(db_params):
    """
    Cria e retorna uma instância de engine para conexão ao banco de dados MySQL.

    Args:
        db_params (dict): Um dicionário contendo as configurações de conexão (user, password, host, port, database).

    Returns:
        sqlalchemy.engine.base.Engine: Uma instância de engine para conexão ao banco de dados MySQL.
                                        Retorna None em caso de erro ao criar a engine.
    """
    try:
        # Modifique a string de conexão para o formato do PyMySQL
        engine = create_engine('mysql+pymysql://{}:{}@{}:{}/{}'.format(
            db_params["user"],
            db_params["password"],
            db_params["host"],
            db_params["port"],
            db_params["database"]
        ))

        return engine

    except Exception as e:
        # Lidar com exceções relacionadas ao SQLAlchemy (por exemplo, problemas ao criar a engine)
        print(f"Erro ao criar engine MySQL: {str(e)}")
        return None
    

def select_db_dw(db_params=None,query=None):
     
    """
    Executa uma consulta SQL no banco de dados usando SQLAlchemy e retorna os resultados como um DataFrame.

    Args:
        db_params (dict): Um dicionário contendo as configurações de conexão do banco de dados.
        query (str): A consulta SQL a ser executada.

    Returns:
        pd.DataFrame or None: Um DataFrame contendo os resultados da consulta SQL.
                              Retorna None em caso de erro na execução da consulta.
    """

    try:
        engine = create_database_engine_dw(db_params)

        df_table = pd.read_sql_query(query, engine)

        engine.dispose()

        return df_table
    
    except exc.SQLAlchemyError as e:
        # Lidar com exceções relacionadas ao SQLAlchemy (por exemplo, problemas na execução da consulta)
        print(f"Erro ao executar consulta SQL: {str(e)}")
        return None


def select_db_source(db_params=None,query=None):
    """
    Executa uma consulta SQL no banco de dados usando o PyMySQL e retorna os resultados como um DataFrame.

    Args:
        db_params (dict): Um dicionário contendo as configurações de conexão do banco de dados.
        query (str): A consulta SQL a ser executada.

    Returns:
        pd.DataFrame or None: Um DataFrame contendo os resultados da consulta SQL.
                              Retorna None em caso de erro na execução da consulta.
    """

    try:
        conn = pymysql.connect(**db_params)
        
        df_table = pd.read_sql_query(query, conn)
        
        conn.close()

        return df_table
    
    except Exception as e:
        # Lidar com exceções (por exemplo, problemas na execução da consulta)
        print(f"Erro ao executar consulta SQL: {str(e)}")
        return None


if __name__=="__main__":

    """
    Exemplo de uso para conectar ao banco de dados, executar uma consulta SQL e imprimir os resultados.
    """

    try:

        db_params = connect_db_source()
        query = "SELECT * FROM raw_data"
        df = select_db_source(db_params=db_params,query=query)

        print(df)

    except Exception as e:
       
        print(f"Erro durante a execução do bloco principal: {str(e)}")