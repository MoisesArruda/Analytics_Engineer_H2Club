# Pacote de extração.

import pandas as pd
import io
from dotenv import load_dotenv
from connect_db import connect_db_source, create_database_engine_source



load_dotenv()

def extract_clientes():

    """
    Extrai os dados do arquivo CSV de clientes e retorna um DataFrame.

    Returns:
        pd.DataFrame or None: Um DataFrame contendo os dados do arquivo CSV.
                              Retorna None em caso de erro na requisição.
    """
    
    try:
        df_clientes = pd.read_csv("data\clientes.csv")
        

    except Exception as e:
        
        print(f'Erro geral: {e}')
        return None


    return df_clientes


def extract_resultados():

    """
    Extrai os dados do arquivo CSV de resultadose retorna um DataFrame.

    Returns:
        pd.DataFrame or None: Um DataFrame contendo os dados do arquivo CSV.
                              Retorna None em caso de erro na requisição.
    """

    try:
        df_resultados = pd.read_csv(r"data\resultado.csv",sep=",",header=0)

    except Exception as e:
        
        print(f'Erro geral: {e}')
        return None


    return df_resultados


def extract_db_source():

    """
    Extrai dados da tabela 'venda' do banco de dados source e retorna um DataFrame.

    Returns:
        pd.DataFrame or None: Um DataFrame contendo os dados da tabela 'venda'.
                              Retorna None em caso de erro na execução da consulta SQL.
    """

    try:

        db_params = connect_db_source()
        engine = create_database_engine_source(db_params)
        query = "Select * from raw_data"
        df_raw_data = pd.read_sql_query(f'{query}', con=engine)
        #cursor.close()
        engine.dispose()

        return df_raw_data
    
    except Exception as e:
        
        print(f'Erro durante a extração de dados do banco de dados source: {e}')
        return None

if __name__=="__main__":

    try:
        print("Extração resultados:")
        print(extract_resultados())

        print("Extração clientes:")
        print(extract_clientes())

        print("Extração banco MySql:")
        print(extract_db_source())

    except Exception as error:
        print(f"Erro durante as extrações: {str(error)}")