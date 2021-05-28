import pandas as pd
import numpy as np
import time


def extract_dim_localidade(conn):
    municipio_sql = '''
        select "Cód.", "Brasil Grande Região Unidade da Federação e Município"
        FROM "STAGED".dados_ibge WHERE "Nível" LIKE 'MU'
        '''

    estado_sql = '''
        select "Cód.", "Brasil Grande Região Unidade da Federação e Município"
        FROM "STAGED".dados_ibge WHERE "Nível" LIKE 'UF'
        '''

    municipio_tbl = pd.read_sql_query(municipio_sql, conn)

    estado_tbl = pd.read_sql_query(estado_sql, conn)

    return municipio_tbl, estado_tbl


def treat_dim_localidade(localidade_tbl, estado_tbl):
    localidade_tbl = localidade_tbl.rename(columns={'Cód.': 'CD_MUNICÍPIO',
        'Brasil Grande Região Unidade da Federação e Município': 'NO_MUNICÍPIO'})

    estado_tbl = estado_tbl.rename(columns={'Cód.': 'CD_ESTADO',
        'Brasil Grande Região Unidade da Federação e Município': 'NO_ESTADO'})

    localidade_tbl['NO_UF'] = localidade_tbl['NO_MUNICÍPIO'].apply(lambda x: x[-3:-1])

    localidade_tbl['NO_MUNICÍPIO'] = localidade_tbl['NO_MUNICÍPIO'].apply(lambda x: x[:-5])

    localidade_tbl['CD_MUNICÍPIO'] = localidade_tbl['CD_MUNICÍPIO'].astype(str)
    localidade_tbl['CD_ESTADO'] = localidade_tbl['CD_MUNICÍPIO'].apply(lambda x: x[:2])
    localidade_tbl['CD_MUNICÍPIO'] = localidade_tbl['CD_MUNICÍPIO'].astype(int)
    localidade_tbl['CD_ESTADO'] = localidade_tbl['CD_ESTADO'].astype(int)

    localidade_tbl = localidade_tbl.merge(estado_tbl, left_on='CD_ESTADO',
        right_on='CD_ESTADO')

    localidade_tbl['SK_LOCALIDADE'] = np.arange(1, len(localidade_tbl))
    return localidade_tbl


def load_dim_localidade(dim_localidade, conn):
    dim_localidade.to_sql(name='D_LOCALIDADE', con=conn, schema='DW',
        if_exists='replace',
        index=False,
        chunksize=40)


def run_dim_localidade(conn_output):
    start_time = time.time()
    município_tbl, estado_tbl = extract_dim_localidade(conn_output)
    extract_time = time.time()
    print('"D_LOCALIDADE" - extract: ', extract_time - start_time)

    dim_localidade = treat_dim_localidade(município_tbl, estado_tbl)
    treat_time = time.time()
    print('treat: ', treat_time - extract_time)

    load_dim_localidade(dim_localidade, conn_output)
    load_time = time.time()
    print('load: ', load_time - treat_time)
