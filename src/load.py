# Pacote de carregamento.

import os
import psycopg2
from transform import transform_clientes,transform_resultado,transform_db
from connect_db import connect_db_dw
import time
from datetime import datetime
import logging


def configurar_log():

    """
    Configura e retorna um objeto Logger para registro de mensagens em um arquivo de log.

    Returns:
        logging.Logger: Objeto Logger configurado para registrar mensagens em um arquivo de log.
                       Retorna None em caso de erro durante a configuração.
    """
    try:

        # Criando a pasta "log" se não existir
        log_folder = 'log'
        os.makedirs(log_folder, exist_ok=True)

        # Obtendo a data atual para incluir no nome do arquivo de log
        data_atual = datetime.now().strftime('%Y%m%d')
        log_file = os.path.join(log_folder, f'arquivo_de_log_{data_atual}.log')

        # Configurando o logging
        logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Retornando o objeto Logger configurado
        return logging.getLogger(__name__)
    
    except Exception as e:
        
        print(f'Erro durante a configuração do log: {e}')
        return None


def load_clientes():

    """
    Carrega dados do DataFrame transformado do arquivo Parquet para o banco de dados data warehouse.

    Retorna None em caso de erro durante o processo de carregamento.
    """
    try:

        # Criando a pasta "log" se não existir
        log_folder = 'log'
        os.makedirs(log_folder, exist_ok=True)

        df_clientes = transform_clientes()
        
        # Criar conexão com o SQL passando os parametros
        db_params = connect_db_dw()
        conexao = psycopg2.connect(**db_params)

        # Criando um cursor e executando um LOOP no DataFrame para fazer o INSERT no SQL
        cursor = conexao.cursor()

        inicio = time.time()

        for row in df_clientes.itertuples(index=False):
            id,sexo,data_nascimento,data_cadastro,cidade,sigla = row
            sql = f"INSERT INTO dim.cliente (id,sexo,data_nascimento,data_cadastro,cidade,sigla) VALUES (%s, %s,%s, %s,%s, %s)"
            val = (id,sexo,data_nascimento,data_cadastro,cidade,sigla)
            cursor.execute(sql, val)
        conexao.commit()

        cursor.close()
        conexao.close()
        final = time.time()


        log = configurar_log()
        log.info(f'Dados carregados do CSV (Clientes): {len(df_clientes)}')
        log.info(f'Tempo de processamento: {int(final - inicio)} segundos')

    except Exception as e:
        print(f'Erro durante o carregamento CSV (Clientes): {e}')


def load_resultado():

    """
    Carrega dados do DataFrame transformado da API para o banco de dados data warehouse.

    Retorna None em caso de erro durante o processo de carregamento.
    """
    try:

        df_resultado = transform_resultado()
        #nome_funcionario = nome_funcionario

        # Criar conexão com o SQL passando os parametros
        db_params = connect_db_dw()
        conexao = psycopg2.connect(**db_params)

        # Criando um cursor e executando um LOOP no DataFrame para fazer o INSERT no SQL
        cursor = conexao.cursor()

        inicio = time.time()

        for row in df_resultado.itertuples(index=False):
            id, data_acesso,clientes_id,buyin,rake,winning = row
            sql = f'Insert into ft.resultado (id, data_acesso,clientes_id,buyin,rake,winning) values (%s,%s,%s,%s,%s,%s)'
            val = (id, data_acesso,clientes_id,buyin,rake,winning)
            cursor.execute(sql, val)
        conexao.commit()

        cursor.close()
        conexao.close()

        final = time.time()

        log = configurar_log()
        log.info(f'Dados carregados do CSV(Resultado): {len(df_resultado)}')
        log.info(f'Tempo de processamento: {int(final - inicio)} segundos')

    except Exception as e:
        print(f'Erro durante o carregamento da API (Funcionario): {e}')


def load_postgres():

    """
    Carrega dados do DataFrame transformado do Postgres para o banco de dados data warehouse.

    Retorna None em caso de erro durante o processo de carregamento.
    """

    try:
        df_consolidated_data = transform_db()
        
        # Criar conexão com o SQL passando os parametros
        db_params = connect_db_dw()
        conexao = psycopg2.connect(**db_params)

        # Criando um cursor e executando um LOOP no DataFrame para fazer o INSERT no SQL
        cursor = conexao.cursor()

        inicio = time.time()

        for row in df_consolidated_data.itertuples(index=False):
            
            id, mes, rake, jogadores, rake_cash_game, rake_torneio, jogadores_cash_game, jogadores_torneio, novos_jogadores = row
            sql = f'INSERT INTO consolidated_data (id, mes, rake, jogadores, rake_cash_game, rake_torneio, jogadores_cash_game, jogadores_torneio, novos_jogadores) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'
            val = (id, mes, rake, jogadores, rake_cash_game, rake_torneio, jogadores_cash_game, jogadores_torneio, novos_jogadores)
  
            cursor.execute(sql, val)

        conexao.commit()
        cursor.close()
        conexao.close()

        final = time.time()

        log = configurar_log()
        log.info(f'Dados carregados do Postgres(consolidated_data): {len(df_consolidated_data)}')
        log.info(f'Tempo de processamento: {int(final - inicio)} segundos')

    except Exception as e:
        print(f'Erro durante o carregamento do Postgres(consolidated_data): {e}')


if __name__=="__main__":

    try:

        load_clientes()
        load_resultado()
        load_postgres()

    except Exception as main_error:
        print(f"Erro durante a execução principal: {str(main_error)}")