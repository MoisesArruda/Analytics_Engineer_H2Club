# Pacote de transformação.

import pandas as pd
from connect_db import connect_db_dw,connect_db_source,create_database_engine_source,create_database_engine_dw
from extract import extract_clientes,extract_resultados
from sqlalchemy.exc import SQLAlchemyError


def transform_clientes():

    """
    Realiza a transformação dos dados, comparando registros do DataFrame extraído com os registros da tabela de clientes no banco de dados.

    Retorna um DataFrame contendo apenas os novos registros que não existem na tabela de clientes.
    """
    try:

        df_clientes = extract_clientes()
        # Substituir valores nulos por uma data padrão
        data_padrao = pd.to_datetime('1900-01-01')  # Data padrão
        df_clientes['data_nascimento'] = df_clientes['data_nascimento'].fillna(data_padrao)

        db_params = connect_db_dw()
        engine = create_database_engine_dw(db_params)
        df_db_clientes = pd.read_sql_query('SELECT * FROM dim.cliente', con=engine)

        engine.dispose()

        novo_registros = df_clientes[~df_clientes['id'].isin(df_db_clientes['id'])]

        return novo_registros
    
    except SQLAlchemyError as e:
        
        print(f"Erro durante a transformação dos dados: {str(e)}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro



def transform_resultado():

    """
    Realiza a transformação dos dados, comparando registros do DataFrame extraído com os registros da tabela de resultados no banco de dados.

    Retorna um DataFrame contendo apenas os novos registros que não existem na tabela de resultados.
    """
    try:
    
        df_resultados = extract_resultados()

         # Adicionando uma coluna de ID e IDs sequenciais começando em 1
        df_resultados["id"] = range(1,len(df_resultados)+1)

        # Reordenando as colunas para mover a coluna "id" para a primeira posição
        cols = df_resultados.columns.tolist()
        cols = ['id'] + [col for col in cols if col != 'id']
        df_resultados = df_resultados[cols]
        
        db_params = connect_db_dw()
        engine = create_database_engine_dw(db_params)
        df_db_resultado = pd.read_sql_query('SELECT * FROM ft.resultado', con=engine)

        engine.dispose()

        novo_registros = df_resultados[~df_resultados['id'].isin(df_db_resultado['id'])]

        return novo_registros
    
    except SQLAlchemyError as e:
        
        print(f"Erro durante a transformação dos dados: {str(e)}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
    


def transform_db():

    """
    Realiza a transformação dos dados, comparando registros do DataFrame extraído do banco de dados source com os registros da tabela de vendas no banco de dados data warehouse.

    Retorna um DataFrame contendo apenas os novos registros que não existem na tabela de vendas do data warehouse.
    """

    try:
        db_params = connect_db_source()


        engine = create_database_engine_source(db_params)
        df_raw_data = pd.read_sql_query("SELECT * from raw_data", con=engine)

        # Adicionando uma coluna de ID e IDs sequenciais começando em 1
        df_raw_data["id"] = range(1, len(df_raw_data) + 1)

        # Reordenando as colunas para mover a coluna "id" para a primeira posição
        cols = df_raw_data.columns.tolist()
        cols = ['id'] + [col for col in cols if col != 'id']
        df_raw_data = df_raw_data[cols]

        formato_data = "%Y-%m-%d"

        # Converter a coluna de data para o formato correto
        df_raw_data['datahora_acesso'] = pd.to_datetime(df_raw_data['datahora_acesso'], format=formato_data, errors='coerce')

        # Verificar se houve algum erro na conversão
        if df_raw_data['datahora_acesso'].isnull().any():
            print("Alguns valores na coluna de datahora_acesso não puderam ser convertidos.")

        # Remover os registros com valores NaT
        df_raw_data = df_raw_data.dropna(subset=['datahora_acesso'])

        # Adicionar coluna 'mes' e agrupar os dados
        df_raw_data['mes'] = df_raw_data['datahora_acesso'].dt.month
    
        df_grouped = df_raw_data.groupby(['id','mes']).agg(
            rake=('rake', 'sum'),
            jogadores=('clientes_id', 'nunique'),
            rake_cash_game=('rake', lambda x: x[df_raw_data['modalidade'] == 'Cash Game'].sum()),
            rake_torneio=('rake', lambda x: x[df_raw_data['modalidade'] == 'Torneio'].sum()),
            jogadores_cash_game=('clientes_id', lambda x: df_raw_data[df_raw_data['modalidade'] == 'Cash Game']['clientes_id'].nunique()),
            jogadores_torneio=('clientes_id', lambda x: df_raw_data[df_raw_data['modalidade'] == 'Torneio']['clientes_id'].nunique()),
            novos_jogadores=('clientes_id', 'nunique')
        )
        
        # Resetar o índice para converter os índices multi-níveis em colunas
        df_grouped_reset = df_grouped.reset_index()

        # Conectar ao banco de dados data warehouse e ler os dados consolidados
        db_params_dw = connect_db_dw()
        engine_dw = create_database_engine_dw(db_params_dw)

        df_consolidated_data = pd.read_sql_query('SELECT * FROM consolidated_data', con=engine_dw)

        # Adicionando uma coluna de ID e IDs sequenciais começando em 1
        df_consolidated_data["id"] = range(1, len(df_consolidated_data) + 1)

        # Reordenando as colunas para mover a coluna "id" para a primeira posição
        cols = df_consolidated_data.columns.tolist()
        cols = ['id'] + [col for col in cols if col != 'id']
        df_consolidated_data = df_consolidated_data[cols]

        novo_registros = df_grouped_reset[~df_grouped_reset['id'].isin(df_consolidated_data['id'])]

        engine.dispose()
        engine_dw.dispose()

        return novo_registros

    except SQLAlchemyError as e:
        
        print(f"Erro durante a transformação dos dados: {str(e)}")
        return pd.DataFrame()  # Retorna um DataFrame vazio em caso de erro
    

    
if __name__=="__main__":

    try:
        print("Transformação clientes:")
        print(transform_clientes())

        print("Transformação resultados:")
        print(transform_resultado())

        print("Transformação banco MySql:")
        print(transform_db())
    
    except Exception as error:
        print(f"Erro durante as transformações: {str(error)}")